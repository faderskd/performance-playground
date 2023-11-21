import os
import typing
from dataclasses import dataclass

from apps.broker.storage_engine import DbSlotPointer

BLOCK_SIZE_BYTES = 4096
INDEX_HEADER_SIZE_BYTES = 4096
MAX_KEYS_LENGTH_BYTES = 1  # max 20, needs validation
MAX_CHILDREN_LENGTH_BYTES = 1  # max 21 (20 + 1)
NODE_POINTER_BLOCK_NUMBER_BYTES = 4  # max 4294967296 nodes in a tree
NODE_KEY_BYTES = 6 # max 2^48 elements
INT_ENCODING = 'big'

@dataclass
class NodePointer:
    block_number: int

    def to_binary(self) -> bytes:
        return int(self.block_number).to_bytes(NODE_POINTER_BLOCK_NUMBER_BYTES, INT_ENCODING)


@dataclass
class BTreeHeaderDto:
    max_keys: int

    def to_binary(self) -> bytes:
        return int(self.max_keys).to_bytes(MAX_KEYS_LENGTH_BYTES, INT_ENCODING)

    @staticmethod
    def from_binary(data: bytes):
        return BTreeHeaderDto(int.from_bytes(data, INT_ENCODING))


@dataclass
class KeyDto:
    identifier: int

    def to_binary(self) -> bytes:
        return int(self.identifier).to_bytes(NODE_KEY_BYTES, INT_ENCODING)


@dataclass
class BTreeNodeDto:
    keys: typing.List[KeyDto]
    children: typing.List[NodePointer]
    values: typing.List[DbSlotPointer]
    max_keys: int

    # TODO: count database capacity
    def to_binary(self) -> bytearray:
        binary_data = bytearray()
        binary_data.append(int(len(self.keys).to_bytes(MAX_KEYS_LENGTH_BYTES, INT_ENCODING)))
        for key in self.keys:
            binary_data.extend(key.to_binary())
        binary_data.append(int(len(self.children).to_bytes(MAX_CHILDREN_LENGTH_BYTES, INT_ENCODING)))
        for node_pointer in self.children:
            binary_data.extend(node_pointer.to_binary())
        binary_data.append(int(len(self.values).to_bytes(MAX_KEYS_LENGTH_BYTES, INT_ENCODING)))
        for value in self.values:
            binary_data.extend(value.to_binary())
        return binary_data

class BTreeBufferPool:
    def __init__(self, file_handle):
        self._file = file_handle
        self._index_blocks = self._number_of_index_blocks()

    def write_header_if_empty(self, header: BTreeHeaderDto) -> None:
        last_offset = self._get_last_offset()
        if last_offset < INDEX_HEADER_SIZE_BYTES:
            binary_header = header.to_binary()
            data_to_save = bytearray(INDEX_HEADER_SIZE_BYTES)
            data_to_save[:len(binary_header)] = binary_header
            self._file.write(data_to_save)

    def get_header(self) -> typing.Optional[BTreeHeaderDto]:
        last_offset = self._get_last_offset()
        if last_offset < INDEX_HEADER_SIZE_BYTES:
            self._file.seek(0)
            return BTreeHeaderDto.from_binary(self._file.read(INDEX_HEADER_SIZE_BYTES))
        return None

    def insert(self, pointer: NodePointer, node: BTreeNodeDto):
        self._file.seek(INDEX_HEADER_SIZE_BYTES + pointer.block_number * BLOCK_SIZE_BYTES)

    def _get_last_offset(self):
        return self._file.seek(0, os.SEEK_END)

    def _number_of_index_blocks(self) -> int:
        last_offset = self._get_last_offset()
        if not last_offset:
            return 0
        return (last_offset - INDEX_HEADER_SIZE_BYTES) // BLOCK_SIZE_BYTES
