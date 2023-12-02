import io
import os
import typing
from dataclasses import dataclass

from apps.broker.storage_engine import DbSlotPointer
from apps.broker.utils import private

BLOCK_SIZE_BYTES = 4096
MAX_KEYS_LENGTH_BYTES = 1  # max 255 keys
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

    def __repr__(self):
        return str(self.key)


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

    # TODO: count database capacity
    def to_binary(self) -> bytes:
        binary_data = io.BytesIO()
        binary_data.write(int(len(self.keys)).to_bytes(MAX_KEYS_LENGTH_BYTES, INT_ENCODING))
        for key in self.keys:
            binary_data.write(key.to_binary())

        for value in self.values:
            binary_data.write(value.to_binary())

        binary_data.write(int(len(self.children)).to_bytes(MAX_KEYS_LENGTH_BYTES, INT_ENCODING))
        for node_pointer in self.children:
            binary_data.write(node_pointer.to_binary())

        binary_data.seek(0)
        return binary_data.read()

    @classmethod
    def from_binary(cls, data: bytes, max_keys: int, node_manager: NodeManager) -> 'PersBTreeNode':
        buff = io.BytesIO(data)
        len_of_keys = int.from_bytes(buff.read(MAX_KEYS_LENGTH_BYTES), INT_ENCODING)
        keys = [PersKey.from_binary(buff) for _ in range(len_of_keys)]
        values = [DbSlotPointer.from_binary(buff) for _ in range(len_of_keys)]
        len_of_children = int.from_bytes(buff.read(MAX_KEYS_LENGTH_BYTES), INT_ENCODING)
        children = [NodePointer.from_binary(buff) for _ in range(len_of_children)]
        if len_of_children:
            return PersBTreeNode(keys, children, values, max_keys, node_manager)

        # it's leaf
        next = NodePointer.from_binary(buff)
        prev = NodePointer.from_binary(buff)
        return PersBTreeNodeLeaf(keys, children, values, max_keys, next, prev, node_manager)


class PersBTreeNodeLeaf(PersBTreeNode):
    def __init__(self, keys: typing.List[PersKey],
                 children: typing.List[NodePointer],
                 values: typing.List[DbSlotPointer],
                 max_keys: int,
                 next: typing.Optional[NodePointer],
                 prev: typing.Optional[NodePointer],
                 node_manager: NodeManager):
        super().__init__(keys, children, values, max_keys, node_manager)
        self.next: NodePointer = next
        self.prev: NodePointer = prev

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

    def insert(self, key: PersKey, value: DbSlotPointer) -> typing.Optional[PersBTreeNode]:
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
            return parent
        return self


class PersBTree:
    def __init__(self, file_handle, max_keys: int):
        self._max_keys = max_keys
        self._node_manager = NodeManager(file_handle, max_keys)
        self.root = self._get_or_create_root()

    def insert(self, key: int, value: DbSlotPointer):
        maybe_new_root = self.root.insert(PersKey(key), value)
        if maybe_new_root:
            self._node_manager.save_node(NodePointer(0), maybe_new_root.to_binary())

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


class DuplicateKeyException(RuntimeError):
    def __init__(self, msg):
        super().__init__(msg)


class PageOverflowException(RuntimeError):
    def __init__(self, msg):
        super().__init__(msg)
