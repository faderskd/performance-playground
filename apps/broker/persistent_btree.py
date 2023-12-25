import io
import os
import typing
from dataclasses import dataclass

from apps.broker.storage_engine import DbSlotPointer
from apps.broker.utils import private

BLOCK_SIZE_BYTES = 4096
MAX_KEYS_LENGTH_BYTES = 1  # max 255 keys
MAX_VALUES_LENGTH_BYTES = 1  # max 255 keys
MAX_CHILDREN_LENGTH_BYTES = 1  # max 255 keys
NODE_POINTER_BLOCK_NUMBER_BYTES = 4  # max 4294967296 nodes in a tree
NODE_KEY_BYTES = 6  # max 2^48 elements
INT_ENCODING = 'big'


@dataclass
class NodePointer:
    block_number: int

    def to_binary(self) -> bytes:
        return int(self.block_number).to_bytes(NODE_POINTER_BLOCK_NUMBER_BYTES, INT_ENCODING, signed=True)

    @staticmethod
    def binary_none() -> bytes:
        return int(-1).to_bytes(NODE_POINTER_BLOCK_NUMBER_BYTES, INT_ENCODING, signed=True)

    @classmethod
    def from_binary(cls, data: io.BytesIO) -> 'NodePointer':
        pointer = int.from_bytes(data.read(NODE_POINTER_BLOCK_NUMBER_BYTES), INT_ENCODING, signed=True)
        if pointer >= 0:
            return NodePointer(pointer)


@private
class NodeManager:
    def __init__(self, file_handle, max_keys: int):
        self._file = file_handle
        self._max_keys = max_keys

    def _seek_to_end(self):
        return self._file.seek(0, os.SEEK_END)

    def save_node(self, pointer: NodePointer, node: bytes):
        if len(node) > BLOCK_SIZE_BYTES:
            raise PageOverflowException(f"Trying to {len(bytes)}, maximum page size is: {BLOCK_SIZE_BYTES}")

        binary_data = bytearray(BLOCK_SIZE_BYTES)
        binary_data[:len(node)] = node

        self._file.seek(pointer.block_number * BLOCK_SIZE_BYTES)
        self._file.write(binary_data)

    def read_node(self, pointer: NodePointer) -> bytes:
        self._file.seek(pointer.block_number * BLOCK_SIZE_BYTES)
        return self._file.read(BLOCK_SIZE_BYTES)

    def read_debug(self, pointer: NodePointer):
        self._file.seek(pointer.block_number * BLOCK_SIZE_BYTES)
        return PersBTreeNode.from_binary(self._file.read(BLOCK_SIZE_BYTES), 3, None)

    def save_new_node(self, node: bytes) -> NodePointer:
        offset = self._seek_to_end()
        new_node_pointer = NodePointer(offset // BLOCK_SIZE_BYTES)
        self.save_node(new_node_pointer, node)
        return new_node_pointer


@dataclass
class PersKey:
    key: int

    def to_binary(self) -> bytes:
        return int(self.key).to_bytes(NODE_KEY_BYTES, INT_ENCODING)

    @classmethod
    def from_binary(cls, data: io.BytesIO) -> 'PersKey':
        return cls(int.from_bytes(data.read(NODE_KEY_BYTES), INT_ENCODING))

    def __gt__(self, other):
        return self.key > other.key

    def __ge__(self, other):
        return self.key >= other.key

    def __repr__(self):
        return str(self.key)


@dataclass
class DeleteResult:
    new_first: typing.Optional[PersKey]
    condition_of_tree_valid: bool = True
    leaf: bool = False
    save: bool = False


class PersBTreeNode:
    def __init__(self, keys: typing.List[PersKey],
                 children: typing.List[NodePointer],
                 values: typing.List[DbSlotPointer],
                 max_keys: int,
                 node_manager: NodeManager):
        self.keys = keys
        self.children = children
        self.values = values
        self._max_keys = max_keys
        self._node_manager = node_manager

    def insert(self, key: PersKey, value: str) -> 'InsertionResult':
        save_curr_node = False
        for i in range(len(self.keys)):
            if self.keys[i] >= key:
                child_node_bytes = self._node_manager.read_node(self.children[i])
                child_node = PersBTreeNode.from_binary(child_node_bytes, self._max_keys, self._node_manager)
                insertion_result = child_node.insert(key, value)
                insert_index = i
                save_index = i
                break
        else:
            child_node_bytes = self._node_manager.read_node(self.children[-1])
            child_node = PersBTreeNode.from_binary(child_node_bytes, self._max_keys, self._node_manager)
            insertion_result = child_node.insert(key, value)
            insert_index = len(self.children)
            save_index = len(self.children) - 1

        if insertion_result.save_curr:
            self._node_manager.save_node(self.children[save_index], insertion_result.updated.to_binary())

        if insertion_result.is_new_node:
            save_curr_node = True
            first_key = insertion_result.updated.keys[0]
            self.keys.insert(insert_index, first_key)
            if insert_index < len(self.children):
                self.children = (self.children[:insert_index] + insertion_result.updated.children +
                                 self.children[insert_index + 1:])
            else:
                self.children.pop()
                self.children.extend(insertion_result.updated.children)

        if len(self.keys) > self._max_keys:
            mid = len(self.keys) // 2
            child_mid = (len(self.children) + 1) // 2
            left_keys, right_keys = self.keys[:mid], self.keys[mid + 1:]
            left_children, right_children = self.children[:child_mid], self.children[child_mid:]
            left_child = PersBTreeNode(left_keys, left_children, [], self._max_keys, self._node_manager)
            right_child = PersBTreeNode(right_keys, right_children, [], self._max_keys, self._node_manager)
            left_child_pointer = self._node_manager.save_new_node(left_child.to_binary())
            right_child_pointer = self._node_manager.save_new_node(right_child.to_binary())

            parent = PersBTreeNode([self.keys[mid]], [left_child_pointer, right_child_pointer], [], self._max_keys,
                                   self._node_manager)
            return InsertionResult(save_curr=False, is_new_node=True, updated=parent) # mark self as garbage
        return InsertionResult(save_curr=save_curr_node, is_new_node=False, updated=self)

    def delete(self, key: PersKey) -> DeleteResult:
        save_curr_node = False

        # search for key to delete
        for i in range(len(self.keys)):
            if self.keys[i] > key:
                child_pointer = self.children[i]
                child = PersBTreeNode.from_binary(self._node_manager.read_node(child_pointer), self._max_keys,
                                                  self._node_manager)
                delete_res = child.delete(key)
                break
        else:
            i = len(self.keys)
            child_pointer = self.children[i]
            child = PersBTreeNode.from_binary(self._node_manager.read_node(child_pointer), self._max_keys,
                                              self._node_manager)
            delete_res = child.delete(key)

        if delete_res.save:
            self._node_manager.save_node(child_pointer, child.to_binary())

        # we deleted from leaf, we are not a parent, we have to replace deleted element (if present) with the inorder successor
        if self._replace_key_if_needed(key, delete_res.new_first):
            save_curr_node = True

        # child has not enough keys/children, so try to borrow from siblings
        borrowed_from_left_child = False
        borrowed_from_right_child = False
        if delete_res.leaf and not delete_res.condition_of_tree_valid:
            if i > 0:
                left_child = PersBTreeNode.from_binary(self._node_manager.read_node(self.children[i - 1]),
                                                       self._max_keys, self._node_manager)
                if left_child._has_enough_to_lend():
                    # borrow right-most key from left child
                    borrowed_right_most_key = left_child.keys.pop()
                    child.keys.insert(0, borrowed_right_most_key)
                    child.values.insert(0, left_child.values.pop())
                    self.keys[i - 1] = borrowed_right_most_key
                    self._node_manager.save_node(self.children[i - 1], left_child.to_binary())
                    self._node_manager.save_node(self.children[i], child.to_binary())
                    save_curr_node = True
                    borrowed_from_left_child = True
            if not borrowed_from_left_child and i + 1 < len(self.children):
                right_child = PersBTreeNodeLeaf.from_binary(self._node_manager.read_node(self.children[i + 1]),
                                                            self._max_keys, self._node_manager)
                if right_child._has_enough_to_lend():
                    # borrow left-most key from right child
                    borrowed_left_most_key = right_child.keys.pop(0)
                    child.keys.append(borrowed_left_most_key)
                    child.values.append(right_child.values.pop(0))
                    self._node_manager.save_node(self.children[i + 1], right_child.to_binary())
                    self._node_manager.save_node(self.children[i], child.to_binary())
                    if i > 0:
                        self.keys[i - 1] = child.keys[0]
                    self.keys[i] = right_child.keys[0]
                    save_curr_node = True
                    borrowed_from_right_child = True
            if not borrowed_from_left_child and not borrowed_from_right_child:
                # we still have invalid child and have to merge
                if i > 0:
                    left_child = PersBTreeNode.from_binary(self._node_manager.read_node(self.children[i - 1]),
                                                           self._max_keys, self._node_manager)
                    # merge with left child
                    new_keys = left_child.keys + child.keys
                    new_values = left_child.values + child.values
                    child.keys = new_keys
                    child.values = new_values
                    left_child.keys = []
                    left_child.values = []
                    # update leaf pointers
                    assert isinstance(left_child, PersBTreeNodeLeaf)
                    assert isinstance(child, PersBTreeNodeLeaf)
                    if left_child.prev:
                        left_child.prev.next = self.children[i]
                    child.prev = left_child.prev
                    self.keys.pop(i - 1)
                    self.children.pop(i - 1)  # TODO mark node as garbage

                    self._node_manager.save_node(self.children[i - 1], left_child.to_binary())
                    self._node_manager.save_node(self.children[i], child.to_binary())
                    save_curr_node = True
                elif i + 1 < len(self.children):
                    # merge with right child
                    right_child = PersBTreeNodeLeaf.from_binary(self._node_manager.read_node(self.children[i + 1]),
                                                                self._max_keys, self._node_manager)
                    new_keys = child.keys + right_child.keys
                    new_values = child.values + right_child.values
                    child.keys = new_keys
                    child.values = new_values
                    right_child.keys = []
                    right_child.values = []
                    # update leaf pointers
                    assert isinstance(right_child, PersBTreeNodeLeaf)
                    assert isinstance(child, PersBTreeNodeLeaf)
                    if right_child.next:
                        right_child.next.prev = self.children[i]
                    child.next = right_child.next

                    self.keys.pop(i)
                    self.children.pop(i + 1)  # TODO mark node as garbage

                    self._node_manager.save_node(self.children[i + 1], right_child.to_binary())
                    self._node_manager.save_node(self.children[i], child.to_binary())
                    save_curr_node = True
            delete_res.new_first = self._get_new_first()
        # try to borrow from sibling being grandfather
        if not delete_res.leaf and not delete_res.condition_of_tree_valid:
            if i > 0:
                left_child = PersBTreeNode.from_binary(self._node_manager.read_node(self.children[i - 1]),
                                                       self._max_keys, self._node_manager)
                if left_child._has_enough_to_lend():
                    # borrow right-most child from left child
                    child.children.insert(0, left_child.children.pop())
                    # it may happen that we need to only override the key or that we need to add a new one
                    if child._has_enough_keys():
                        child.keys[0] = self.keys[i - 1]
                    else:
                        child.keys.insert(0, self.keys[i - 1])
                    self.keys[i - 1] = left_child.keys.pop()
                    self._node_manager.save_node(self.children[i - 1], left_child.to_binary())
                    self._node_manager.save_node(self.children[i], child.to_binary())
                    save_curr_node = True
                    borrowed_from_left_child = True
            if not borrowed_from_left_child and i + 1 < len(self.children):
                right_child = PersBTreeNodeLeaf.from_binary(self._node_manager.read_node(self.children[i + 1]),
                                                            self._max_keys, self._node_manager)
                if right_child._has_enough_to_lend():
                    # borrow left-most key from right child
                    child.children.append(right_child.children.pop(0))
                    # it may happen that we need to only override the key or that we need to add a new one
                    if child._has_enough_keys():
                        child.keys[-1] = self.keys[i]
                    else:
                        child.keys.append(self.keys[i])
                    self.keys[i] = right_child.keys.pop(0)
                    self._node_manager.save_node(self.children[i + 1], right_child.to_binary())
                    self._node_manager.save_node(self.children[i], child.to_binary())
                    save_curr_node = True
                    borrowed_from_right_child = True
            if not borrowed_from_left_child and not borrowed_from_right_child:
                # we still have the invalid child and have to merge
                if i > 0:
                    left_child = PersBTreeNode.from_binary(self._node_manager.read_node(self.children[i - 1]),
                                                           self._max_keys, self._node_manager)
                    # merge with left child
                    new_children = left_child.children + child.children
                    new_keys = left_child.keys + [self.keys.pop(i - 1)] + child.keys
                    left_child.children = new_children
                    left_child.keys = new_keys
                    self.children.pop(i)  # TODO mark node as garbage
                    self._node_manager.save_node(self.children[i - 1], left_child.to_binary())
                    save_curr_node = True
                elif i + 1 < len(self.children):
                    # merge with right child
                    right_child = PersBTreeNodeLeaf.from_binary(self._node_manager.read_node(self.children[i + 1]),
                                                                self._max_keys, self._node_manager)

                    new_children = child.children + right_child.children
                    new_keys = child.keys + [self.keys.pop(i)] + right_child.keys
                    right_child.children = new_children
                    right_child.keys = new_keys
                    self.children.pop(i)  # TODO mark node as garbage
                    self._node_manager.save_node(self.children[i + 1], right_child.to_binary())
                    save_curr_node = True
                else:
                    print("Impossibru...")

        # we are a parent, we deleted from leaf and tried to restore the tree condition
        delete_res.condition_of_tree_valid = self.is_at_least_half_full()
        delete_res.leaf = False
        delete_res.save = save_curr_node
        return delete_res

    def _replace_key_if_needed(self, old: PersKey, new: PersKey):
        for i in range(len(self.keys)):
            if self.keys[i] == old:
                self.keys[i] = new
                return True

    def _get_new_first(self) -> typing.Optional[PersKey]:
        if self.children:
            leftmost_child_binary = self._node_manager.read_node(self.children[0])
            leftmost_child = PersBTreeNodeLeaf.from_binary(leftmost_child_binary, self._max_keys, self._node_manager)
            return leftmost_child.keys[0]
        return None

    def _has_enough_to_lend(self):
        return len(self.keys) > self._max_keys // 2

    def _has_enough_keys(self):
        return len(self.keys) >= self._max_keys // 2

    def is_at_least_half_full(self):
        return len(self.keys) >= self._max_keys // 2 and len(self.children) > self._max_keys // 2

    # TODO: count database capacity
    def to_binary(self) -> bytes:
        binary_data = io.BytesIO()
        binary_data.write(int(len(self.keys)).to_bytes(MAX_KEYS_LENGTH_BYTES, INT_ENCODING))
        for key in self.keys:
            binary_data.write(key.to_binary())

        binary_data.write(int(len(self.values)).to_bytes(MAX_VALUES_LENGTH_BYTES, INT_ENCODING))
        for value in self.values:
            binary_data.write(value.to_binary())

        binary_data.write(int(len(self.children)).to_bytes(MAX_CHILDREN_LENGTH_BYTES, INT_ENCODING))
        for node_pointer in self.children:
            binary_data.write(node_pointer.to_binary())

        binary_data.seek(0)
        return binary_data.read()

    @classmethod
    def from_binary(cls, data: bytes, max_keys: int, node_manager: NodeManager) -> 'PersBTreeNode':
        buff = io.BytesIO(data)
        len_of_keys = int.from_bytes(buff.read(MAX_KEYS_LENGTH_BYTES), INT_ENCODING)
        keys = [PersKey.from_binary(buff) for _ in range(len_of_keys)]
        len_of_values = int.from_bytes(buff.read(MAX_VALUES_LENGTH_BYTES), INT_ENCODING)
        values = [DbSlotPointer.from_binary(buff) for _ in range(len_of_values)]
        len_of_children = int.from_bytes(buff.read(MAX_CHILDREN_LENGTH_BYTES), INT_ENCODING)
        children = [NodePointer.from_binary(buff) for _ in range(len_of_children)]
        if len_of_children:
            return PersBTreeNode(keys, children, values, max_keys, node_manager)

        # it's leaf
        next = NodePointer.from_binary(buff)
        prev = NodePointer.from_binary(buff)
        return PersBTreeNodeLeaf(keys, children, values, max_keys, next, prev, node_manager)

    def __repr__(self):
        return str(self.keys)


class PersBTreeNodeLeaf(PersBTreeNode):
    def __init__(self, keys: typing.List[PersKey],
                 children: typing.List[NodePointer],
                 values: typing.List[DbSlotPointer],
                 max_keys: int,
                 next: typing.Optional[NodePointer],
                 prev: typing.Optional[NodePointer],
                 node_manager: NodeManager):
        super().__init__(keys, children, values, max_keys, node_manager)
        self.next: typing.Optional[NodePointer] = next
        self.prev: typing.Optional[NodePointer] = prev

    def to_binary(self) -> bytes:
        binary_data = io.BytesIO()
        binary_data.write(super().to_binary())
        if self.next:
            binary_data.write(self.next.to_binary())
        else:
            binary_data.write(NodePointer.binary_none())
        if self.prev:
            binary_data.write(self.prev.to_binary())
        else:
            binary_data.write(NodePointer.binary_none())
        binary_data.seek(0)
        return binary_data.read()

    def insert(self, key: PersKey, value: DbSlotPointer) -> 'InsertionResult':
        for i in range(len(self.keys)):
            if self.keys[i] == key:
                raise DuplicateKeyException(f"Duplicate key {key}")
            if self.keys[i] > key:
                self.keys.insert(i, key)
                self.values.insert(i, value)
                break
        else:
            self.keys.append(key)
            self.values.append(value)

        if len(self.keys) > self._max_keys:
            mid = len(self.keys) // 2
            left_keys, right_keys = self.keys[:mid], self.keys[mid:]
            left_values, right_values = self.values[:mid], self.values[mid:]
            left_child = PersBTreeNodeLeaf(left_keys, [], left_values, self._max_keys, None, None, self._node_manager)
            right_child = PersBTreeNodeLeaf(right_keys, [], right_values, self._max_keys, None, None,
                                            self._node_manager)
            left_child_pointer = self._node_manager.save_new_node(left_child.to_binary())
            right_child_pointer = self._node_manager.save_new_node(right_child.to_binary())

            left_child.next = right_child_pointer
            left_child.prev = self.prev
            right_child.prev = left_child_pointer
            right_child.next = self.next
            if self.prev:
                self.prev.next = left_child_pointer
            if self.next:
                self.next.prev = right_child_pointer

            parent = PersBTreeNode([self.keys[mid]], [left_child_pointer, right_child_pointer],
                                   [], self._max_keys, self._node_manager)
            return InsertionResult(save_curr=False, is_new_node=True, updated=parent)
        return InsertionResult(save_curr=True, is_new_node=False, updated=self)

    def delete(self, key: PersKey) -> DeleteResult:
        for i in range(len(self.keys)):
            if self.keys[i] == key:
                self.keys.pop(i)
                self.values.pop(i)
                break
        else:
            raise NoSuchKeyException(f'No key {key} found in a tree')

        if self.is_at_least_half_full():
            # case when after deletion b+tree condition is maintained in leaf, nothing to do more
            return DeleteResult(new_first=self.keys[0], condition_of_tree_valid=True, leaf=True, save=True)
        # case when there is not enough elements in leaf after deletion
        if not self.keys:
            return DeleteResult(new_first=None, condition_of_tree_valid=False, leaf=True, save=False)
        # there are still some keys available
        return DeleteResult(new_first=self.keys[0], condition_of_tree_valid=False, leaf=True, save=False)

    def is_at_least_half_full(self):
        return len(self.keys) >= self._max_keys // 2


@dataclass
class InsertionResult:
    save_curr: bool
    is_new_node: bool
    updated: PersBTreeNode


# TODO make it auto-closable
class PersBTree:
    def __init__(self, index_file_path: str, max_keys: int):
        self._max_keys = max_keys
        self.file_handle = self._get_or_create_index_file(index_file_path)
        self._node_manager = NodeManager(self.file_handle, max_keys)
        self.root = self._get_or_create_root()

    def insert(self, key: int, value: DbSlotPointer):
        result = self.root.insert(PersKey(key), value)
        if result.save_curr or result.is_new_node:
            self._node_manager.save_node(NodePointer(0), result.updated.to_binary())
        self.root = result.updated

    def delete(self, key: int) -> None:
        result = self.root.delete(PersKey(key))
        if len(self.root.keys) in [0, 1] and len(self.root.children) == 1:
            first_child = PersBTreeNode.from_binary(self._node_manager.read_node(self.root.children[0]), self._max_keys,
                                                    self._node_manager) # TODO mark node as garbage
            self._node_manager.save_node(NodePointer(0), first_child.to_binary())
        elif not self.root.keys and not self.root.children:
            self.root = PersBTreeNodeLeaf([], [], [], self._max_keys, None, None, self._node_manager)
            self._node_manager.save_node(NodePointer(0), self.root.to_binary())
        elif result.save:
            self._node_manager.save_node(NodePointer(0), self.root.to_binary())

    def _get_or_create_root(self):
        node_bytes = self._node_manager.read_node(NodePointer(0))
        if len(node_bytes) == 0:
            root = PersBTreeNodeLeaf([], [], [], self._max_keys, None, None, self._node_manager)
            self._node_manager.save_node(NodePointer(0), root.to_binary())
        else:
            root = self._parse_node(node_bytes)
        return root

    def _save_node(self, pointer: NodePointer, node: PersBTreeNode):
        self._node_manager.save_node(pointer, node.to_binary())

    def _parse_node(self, node_bytes: bytes) -> PersBTreeNode:
        return PersBTreeNode.from_binary(node_bytes, self._max_keys, self._node_manager)

    def dfs(self) -> typing.List[int]:
        dfs_container = []
        self._dfs_helper(self.root, dfs_container)
        return dfs_container

    def _dfs_helper(self, node: PersBTreeNode, container: typing.List):
        if not node:
            return []
        container.extend([pers_key.key for pers_key in node.keys])
        for pointer in node.children:
            child_node = PersBTreeNode.from_binary(self._node_manager.read_node(pointer), self._max_keys,
                                                   self._node_manager)
            self._dfs_helper(child_node, container)

    @staticmethod
    def _get_or_create_index_file(file_path):
        if not os.path.exists(file_path):
            with open(file_path, 'w+') as file:
                pass
        return open(file_path, 'r+b')


class DuplicateKeyException(RuntimeError):
    def __init__(self, msg):
        super().__init__(msg)


class PageOverflowException(RuntimeError):
    def __init__(self, msg):
        super().__init__(msg)


class NoSuchKeyException(Exception):
    def __init__(self, msg: str):
        super().__init__(msg)
