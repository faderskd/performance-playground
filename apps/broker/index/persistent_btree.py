import io
import os
import threading
import typing
from dataclasses import dataclass
from enum import Enum

from apps.broker.index.lock_manager import LockManager
from apps.broker.index.persistent_data import PagePointer, PersKey
from apps.broker.storage_engine import DbRecordPointer, INT_ENCODING

MAX_KEYS_LENGTH_BYTES = 1  # max 255 keys
MAX_VALUES_LENGTH_BYTES = 1  # max 255 keys
MAX_CHILDREN_LENGTH_BYTES = 1  # max 255 keys


class LockType(Enum):
    READ = 1
    WRITE = 2


class LockState:
    def __init__(self, lock: threading.Lock, acquired: bool):
        self.lock = lock

    def release(self):
        self.lock.release()


class LockContext:
    def __init__(self):
        self.parent_locks: typing.List[threading.Lock] = []

    def push(self, lock: threading.Lock) -> int:
        self.parent_locks.append(lock)
        return len(self.parent_locks) - 1

    def release_and_remove(self, index: int):
        if len(self.parent_locks) > index:
            self.parent_locks[index].release()
            self.parent_locks.pop(index)

    def release_and_remove_parent_locks(self, index: int):
        for p in range(index):
            self.parent_locks[p].release()
        self.parent_locks = self.parent_locks[index:]


@dataclass
class DeleteResult:
    new_first: typing.Optional[PersKey]
    condition_of_tree_valid: bool = True
    leaf: bool = False


class PersBTreeNode:
    def __init__(self, pointer: typing.Optional[PagePointer],
                 keys: typing.List[PersKey],
                 children: typing.List[PagePointer],
                 values: typing.List[DbRecordPointer],
                 max_keys: int,
                 page_manager,
                 lock_manager: LockManager):
        self.pointer = pointer
        self.keys = keys
        self.children = children
        self.values = values
        self._max_keys = max_keys
        self._page_manager = page_manager
        self._lock_manager = lock_manager

    def insert(self, key: PersKey, value: DbRecordPointer, lock_ctx: LockContext) -> 'InsertionResult':
        print(f"{threading.current_thread().name} -> enter Node {self.pointer.block_number} insert key {key}")
        save_curr_node = False
        curr_node_lock = self._lock_manager.get_lock(self.pointer)
        curr_node_lock.acquire()
        lock_index = lock_ctx.push(curr_node_lock)
        try:
            if self._can_release_parents_locks():
                lock_ctx.release_and_remove_parent_locks(lock_index)
            for i in range(len(self.keys)):
                if self.keys[i] >= key:
                    # TODO: needs acquire
                    child_node = self._page_manager.read_page(self.children[i])
                    insertion_result = child_node.insert(key, value, lock_ctx)
                    insert_index = i
                    break
            else:
                # TODO: needs acquire
                child_node = self._page_manager.read_page(self.children[-1])
                insertion_result = child_node.insert(key, value, lock_ctx)
                insert_index = len(self.children)

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
                left_child = PersBTreeNode(None, left_keys, left_children, [], self._max_keys, self._page_manager,
                                           self._lock_manager)
                right_child = PersBTreeNode(None, right_keys, right_children, [], self._max_keys,
                                            self._page_manager,
                                            self._lock_manager)
                left_child = self._page_manager.save_new_page(left_child)
                right_child = self._page_manager.save_new_page(right_child)

                parent = PersBTreeNode(self.pointer, [self.keys[mid]], [left_child.pointer, right_child.pointer],
                                       [], self._max_keys, self._page_manager, self._lock_manager)
                print(f"{threading.current_thread().name} -> exit Node {self.pointer.block_number} insert key {key}")
                return InsertionResult(is_new_node=True, updated=parent)  # mark self as garbage
            if save_curr_node:
                self._page_manager.save_page(self)
            print(f"{threading.current_thread().name} -> exit Node {self.pointer.block_number} insert key {key}")
            return InsertionResult(is_new_node=False, updated=self)
        finally:
            lock_ctx.release_and_remove(lock_index)

    def delete(self, key: PersKey) -> DeleteResult:
        save_curr_node = False

        # search for key to delete
        for i in range(len(self.keys)):
            if self.keys[i] > key:
                child_pointer = self.children[i]
                child = self._page_manager.read_page(child_pointer)
                delete_res = child.delete(key)
                break
        else:
            i = len(self.keys)
            child_pointer = self.children[i]
            child = self._page_manager.read_page(child_pointer)
            delete_res = child.delete(key)

        # we deleted from leaf, we are not a parent, we have to replace deleted element (if present) with the inorder successor
        if self._replace_key_if_needed(key, delete_res.new_first):
            save_curr_node = True

        # child has not enough keys/children, so try to borrow from siblings
        borrowed_from_left_child = False
        borrowed_from_right_child = False
        if delete_res.leaf and not delete_res.condition_of_tree_valid:
            if i > 0:
                left_child = self._page_manager.read_page(self.children[i - 1])
                if left_child._has_enough_to_lend():
                    # borrow right-most key from left child
                    borrowed_right_most_key = left_child.keys.pop()
                    child.keys.insert(0, borrowed_right_most_key)
                    child.values.insert(0, left_child.values.pop())
                    self.keys[i - 1] = borrowed_right_most_key
                    self._page_manager.save_page(left_child)
                    self._page_manager.save_page(child)
                    save_curr_node = True
                    borrowed_from_left_child = True
            if not borrowed_from_left_child and i + 1 < len(self.children):
                right_child = self._page_manager.read_page(self.children[i + 1])
                if right_child._has_enough_to_lend():
                    # borrow left-most key from right child
                    borrowed_left_most_key = right_child.keys.pop(0)
                    child.keys.append(borrowed_left_most_key)
                    child.values.append(right_child.values.pop(0))
                    self._page_manager.save_page(right_child)
                    self._page_manager.save_page(child)
                    if i > 0:
                        self.keys[i - 1] = child.keys[0]
                    self.keys[i] = right_child.keys[0]
                    save_curr_node = True
                    borrowed_from_right_child = True
            if not borrowed_from_left_child and not borrowed_from_right_child:
                # we still have invalid child and have to merge
                if i > 0:
                    left_child = self._page_manager.read_page(self.children[i - 1])
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
                        prev_child = self._page_manager.read_page(left_child.prev)
                        assert isinstance(prev_child, PersBTreeNodeLeaf)
                        prev_child.next = self.children[i]
                        self._page_manager.save_page(prev_child)
                    child.prev = left_child.prev

                    self._page_manager.save_page(child)
                    save_curr_node = True
                    self.keys.pop(i - 1)
                    self.children.pop(i - 1)  # TODO mark node as garbage
                elif i + 1 < len(self.children):
                    # merge with right child
                    right_child = self._page_manager.read_page(self.children[i + 1])
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
                        next_child = self._page_manager.read_page(right_child.next)
                        assert isinstance(next_child, PersBTreeNodeLeaf)
                        next_child.prev = self.children[i]
                        self._page_manager.save_page(next_child)
                    child.next = right_child.next

                    self._page_manager.save_page(child)
                    save_curr_node = True

                    self.keys.pop(i)
                    self.children.pop(i + 1)  # TODO mark node as garbage
            delete_res.new_first = self._get_new_first()
        # try to borrow from sibling being grandfather
        if not delete_res.leaf and not delete_res.condition_of_tree_valid:
            if i > 0:
                left_child = self._page_manager.read_page(self.children[i - 1])
                if left_child._has_enough_to_lend():
                    # borrow right-most child from left child
                    child.children.insert(0, left_child.children.pop())
                    # it may happen that we need to only override the key or that we need to add a new one
                    if child._has_enough_keys():
                        child.keys[0] = self.keys[i - 1]
                    else:
                        child.keys.insert(0, self.keys[i - 1])
                    self.keys[i - 1] = left_child.keys.pop()
                    self._page_manager.save_page(left_child)
                    self._page_manager.save_page(child)
                    save_curr_node = True
                    borrowed_from_left_child = True
            if not borrowed_from_left_child and i + 1 < len(self.children):
                right_child = self._page_manager.read_page(self.children[i + 1])
                if right_child._has_enough_to_lend():
                    # borrow left-most key from right child
                    child.children.append(right_child.children.pop(0))
                    # it may happen that we need to only override the key or that we need to add a new one
                    if child._has_enough_keys():
                        child.keys[-1] = self.keys[i]
                    else:
                        child.keys.append(self.keys[i])
                    self.keys[i] = right_child.keys.pop(0)
                    self._page_manager.save_page(right_child)
                    self._page_manager.save_page(child)
                    save_curr_node = True
                    borrowed_from_right_child = True
            if not borrowed_from_left_child and not borrowed_from_right_child:
                # we still have the invalid child and have to merge
                if i > 0:
                    left_child = self._page_manager.read_page(self.children[i - 1])
                    # merge with left child
                    new_children = left_child.children + child.children
                    new_keys = left_child.keys + [self.keys.pop(i - 1)] + child.keys
                    left_child.children = new_children
                    left_child.keys = new_keys
                    self._page_manager.save_page(left_child)
                    save_curr_node = True
                    self.children.pop(i)  # TODO mark node as garbage
                elif i + 1 < len(self.children):
                    # merge with right child
                    right_child = self._page_manager.read_page(self.children[i + 1])
                    new_children = child.children + right_child.children
                    new_keys = child.keys + [self.keys.pop(i)] + right_child.keys
                    right_child.children = new_children
                    right_child.keys = new_keys
                    self._page_manager.save_page(right_child)
                    save_curr_node = True
                    self.children.pop(i)  # TODO mark node as garbage
                else:
                    print("Impossibru...")

        # we are a parent, we deleted from leaf and tried to restore the tree condition
        delete_res.condition_of_tree_valid = self._is_at_least_half_full()
        delete_res.leaf = False
        if save_curr_node:
            self._page_manager.save_page(self)
        return delete_res

    def find(self, key: PersKey) -> DbRecordPointer:
        for i in range(len(self.keys)):
            if self.keys[i] > key:
                child = self._page_manager.read_page(self.children[i])
                return child.find(key)
        else:
            i = len(self.keys)
            child = self._page_manager.read_page(self.children[i])
            res = child.find(key)
        return res

    def _replace_key_if_needed(self, old: PersKey, new: PersKey):
        for i in range(len(self.keys)):
            if self.keys[i] == old:
                self.keys[i] = new
                return True

    def _get_new_first(self) -> typing.Optional[PersKey]:
        if self.children:
            leftmost_child = self._page_manager.read_page(self.children[0])
            return leftmost_child.keys[0]
        return None

    def _has_enough_to_lend(self):
        return len(self.keys) > self._max_keys // 2

    def _has_enough_keys(self):
        return len(self.keys) >= self._max_keys // 2

    def _is_at_least_half_full(self):
        return len(self.keys) >= self._max_keys // 2 and len(self.children) > self._max_keys // 2

    def is_empty(self):
        return len(self.keys) == 0 and len(self.values) == 0 and len(self.children) == 0

    def _can_release_parents_locks(self):
        if self._is_root():
            return False
        return len(self.keys) < self._max_keys

    def _is_root(self):
        return self.pointer.block_number == 0

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
    def from_binary(cls, pointer: PagePointer, data: bytes, max_keys: int, node_manager,
                    lock_manager) -> 'PersBTreeNode':
        buff = io.BytesIO(data)
        len_of_keys = int.from_bytes(buff.read(MAX_KEYS_LENGTH_BYTES), INT_ENCODING)
        keys = [PersKey.from_binary(buff) for _ in range(len_of_keys)]
        len_of_values = int.from_bytes(buff.read(MAX_VALUES_LENGTH_BYTES), INT_ENCODING)
        values = [DbRecordPointer.from_binary(buff) for _ in range(len_of_values)]
        len_of_children = int.from_bytes(buff.read(MAX_CHILDREN_LENGTH_BYTES), INT_ENCODING)
        children = [PagePointer.from_binary(buff) for _ in range(len_of_children)]
        if len_of_children:
            return PersBTreeNode(pointer, keys, children, values, max_keys, node_manager, lock_manager)

        # it's leaf
        next = PagePointer.from_binary(buff)
        prev = PagePointer.from_binary(buff)
        return PersBTreeNodeLeaf(pointer, keys, children, values, max_keys, next, prev, node_manager, lock_manager)

    def __repr__(self):
        return str(self.keys)


class PersBTreeNodeLeaf(PersBTreeNode):
    def __init__(self, pointer: typing.Optional[PagePointer],
                 keys: typing.List[PersKey],
                 children: typing.List[PagePointer],
                 values: typing.List[DbRecordPointer],
                 max_keys: int,
                 next: typing.Optional[PagePointer],
                 prev: typing.Optional[PagePointer],
                 page_manager,
                 lock_manager: LockManager):
        super().__init__(pointer, keys, children, values, max_keys, page_manager, lock_manager)
        self.next: typing.Optional[PagePointer] = next
        self.prev: typing.Optional[PagePointer] = prev

    def to_binary(self) -> bytes:
        binary_data = io.BytesIO()
        binary_data.write(super().to_binary())
        if self.next:
            binary_data.write(self.next.to_binary())
        else:
            binary_data.write(PagePointer.binary_none())
        if self.prev:
            binary_data.write(self.prev.to_binary())
        else:
            binary_data.write(PagePointer.binary_none())
        binary_data.seek(0)
        return binary_data.read()

    def insert(self, key: PersKey, value: DbRecordPointer, lock_ctx: LockContext) -> 'InsertionResult':
        print(f"{threading.current_thread().name} -> enter Leaf {self.pointer.block_number}  insert key {key}")
        curr_node_lock = self._lock_manager.get_lock(self.pointer)
        curr_node_lock.acquire()
        lock_index = lock_ctx.push(curr_node_lock)
        try:
            if self._can_release_parents_locks():
                lock_ctx.release_and_remove_parent_locks(lock_index)
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
                left_child = PersBTreeNodeLeaf(None, left_keys, [], left_values, self._max_keys, None, None,
                                               self._page_manager, self._lock_manager)
                right_child = PersBTreeNodeLeaf(None, right_keys, [], right_values, self._max_keys, None, None,
                                                self._page_manager, self._lock_manager)
                left_child = self._page_manager.save_new_page(left_child)
                right_child= self._page_manager.save_new_page(right_child)

                # TODO: lock when updating pointers
                left_child.next = right_child.pointer
                left_child.prev = self.prev
                right_child.prev = left_child.pointer
                right_child.next = self.next
                if self.prev:
                    prev_child = self._page_manager.read_page(self.prev)
                    assert isinstance(prev_child, PersBTreeNodeLeaf)
                    prev_child.next = left_child.pointer
                    self._page_manager.save_page(prev_child)
                if self.next:
                    next_child = self._page_manager.read_page(self.next)
                    assert isinstance(next_child, PersBTreeNodeLeaf)
                    next_child.prev = right_child.pointer
                    self._page_manager.save_page(next_child)

                self._page_manager.save_page(left_child)
                self._page_manager.save_page(right_child)

                parent = PersBTreeNode(self.pointer, [self.keys[mid]], [left_child.pointer, right_child.pointer],
                                       [], self._max_keys, self._page_manager, self._lock_manager)
                print(f"{threading.current_thread().name} -> exit Leaf {self.pointer.block_number} insert key {key}")
                return InsertionResult(is_new_node=True, updated=parent)
            print(f"{threading.current_thread().name} -> exit Leaf {self.pointer.block_number} insert key {key}")
            self._page_manager.save_page(self)
            return InsertionResult(is_new_node=False, updated=self)
        finally:
            lock_ctx.release_and_remove(lock_index)

    def delete(self, key: PersKey) -> DeleteResult:
        for i in range(len(self.keys)):
            if self.keys[i] == key:
                self.keys.pop(i)
                self.values.pop(i)
                break
        else:
            raise NoSuchKeyException(f'No key {key} found in a tree')

        if self._is_at_least_half_full():
            # case when after deletion b+tree condition is maintained in leaf, nothing to do more
            self._page_manager.save_page(self)
            return DeleteResult(new_first=self.keys[0], condition_of_tree_valid=True, leaf=True)
        # case when there is not enough elements in leaf after deletion
        if not self.keys:
            return DeleteResult(new_first=None, condition_of_tree_valid=False, leaf=True)
        # there are still some keys available
        return DeleteResult(new_first=self.keys[0], condition_of_tree_valid=False, leaf=True)

    def find(self, key: PersKey) -> DbRecordPointer:
        for i in range(len(self.keys)):
            if self.keys[i] == key:
                return self.values[i]
        raise NoSuchKeyException(f'No key {key} found in a tree')

    def _is_at_least_half_full(self):
        return len(self.keys) >= self._max_keys // 2


@dataclass
class InsertionResult:
    is_new_node: bool
    updated: typing.Optional[PersBTreeNode]
    insufficient_lock_permissions: bool = False


# TODO make it auto-closable
class PersBTree:
    def __init__(self, index_file_path: str, max_keys: int):
        from apps.broker.index.page_manager import PageManager

        self._max_keys = max_keys
        self._file_handle = self._get_or_create_index_file(index_file_path)
        self._page_manager = PageManager(self._file_handle, max_keys)
        self._lock_manager = LockManager()
        self._root = self._get_or_create_root()
        self._global_lock = threading.Lock()

    def insert(self, key: int, value: DbRecordPointer):
        self._global_lock.acquire()
        lock_ctx = LockContext()
        lock_index = lock_ctx.push(self._global_lock)
        try:
            result = self._root.insert(PersKey(key), value, lock_ctx)
            if result.is_new_node:
                self._root = result.updated
                self._page_manager.save_page(result.updated)
        finally:
            lock_ctx.release_and_remove(lock_index)

    def delete(self, key: int) -> None:
        self._root.delete(PersKey(key))
        if len(self._root.keys) in [0, 1] and len(self._root.children) == 1:
            first_child = self._page_manager.read_page(self._root.children[0])  # TODO mark node as garbage
            self._page_manager.save_page(first_child)
            self._root = first_child
        elif not self._root.keys and not self._root.children:
            self._root = PersBTreeNodeLeaf(PagePointer(0), [], [], [], self._max_keys, None, None, self._page_manager,
                                           self._lock_manager)
            self._page_manager.save_page(self._root)

    def find(self, key: int) -> DbRecordPointer:
        return self._root.find(PersKey(key))

    def get_leafs(self) -> typing.List[PersKey]:
        sorted_keys = []
        curr_node = self._root
        while curr_node.children:
            curr_node = self._page_manager.read_page(curr_node.children[0])
        assert isinstance(curr_node, PersBTreeNodeLeaf)
        while curr_node.keys:
            sorted_keys.extend(curr_node.keys)
            if curr_node.next:
                curr_node = self._page_manager.read_page(curr_node.next)
            else:
                break
        return sorted_keys

    def _get_or_create_root(self):
        root = self._page_manager.read_page_or_get_empty(PagePointer(0))
        if root.is_empty():
            root = self._page_manager.save_new_page(root)
        return root

    def dfs(self) -> typing.List[int]:
        dfs_container = []
        self._dfs_helper(self._root, dfs_container)
        return dfs_container

    def _dfs_helper(self, node: PersBTreeNode, container: typing.List):
        if not node:
            return []
        container.extend([pers_key.key for pers_key in node.keys])
        for pointer in node.children:
            child_node = self._page_manager.read_page(pointer)
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


class NoSuchKeyException(Exception):
    def __init__(self, msg: str):
        super().__init__(msg)
