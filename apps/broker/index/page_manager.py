import os
import threading

from apps.broker.index.lock_manager import LockManager
from apps.broker.index.persistent_btree import PersBTreeNode, PersBTreeNodeLeaf
from apps.broker.index.persistent_data import PagePointer
from apps.broker.utils import private

BLOCK_SIZE_BYTES = 4096


@private  # TODO make it auto-closable and flush on cleanup
class PageManager:
    def __init__(self, file_handle, max_keys: int):
        self._file = file_handle
        self._max_keys = max_keys
        # self._cache = {}  # make it lfu cache
        self._lock = threading.Lock()
        self._lock_manager = LockManager()

    def save_page(self, node: 'PersBTreeNode'):
        with self._lock:
            assert node.pointer is not None
            node_binary = node.to_binary()
            if len(node_binary) > BLOCK_SIZE_BYTES:
                raise PageOverflowException(f"Trying to {len(bytes)}, maximum page size is: {BLOCK_SIZE_BYTES}")

            binary_data = bytearray(BLOCK_SIZE_BYTES)
            binary_data[:len(node_binary)] = node_binary

            # save to file only when cache space needs to be freed
            self._file.seek(node.pointer.block_number * BLOCK_SIZE_BYTES)
            self._file.write(binary_data)
            # self._cache[node.pointer] = node

    def read_page(self, pointer: PagePointer) -> 'PersBTreeNode':
        with self._lock:
            # if pointer not in self._cache:
            self._file.seek(pointer.block_number * BLOCK_SIZE_BYTES)
            data = self._file.read(BLOCK_SIZE_BYTES)
            data = PersBTreeNode.from_binary(pointer, data, self._max_keys, self, self._lock_manager)
            return data

    def read_page_or_get_empty(self, pointer: PagePointer) -> 'PersBTreeNode':
        with self._lock:
            # if pointer not in self._cache:
            self._file.seek(pointer.block_number * BLOCK_SIZE_BYTES)
            data = self._file.read(BLOCK_SIZE_BYTES)
            if len(data) == 0:
                data = PersBTreeNodeLeaf(pointer, [], [], [], self._max_keys,
                                         None, None, self, self._lock_manager)
            else:
                data = PersBTreeNode.from_binary(pointer, data, self._max_keys, self,
                                                 self._lock_manager)
            return data

    def read_debug(self, pointer: PagePointer):
        self._file.seek(pointer.block_number * BLOCK_SIZE_BYTES)
        return PersBTreeNode.from_binary(pointer, self._file.read(BLOCK_SIZE_BYTES), 3, None, None)

    def save_new_page(self, node: 'PersBTreeNode') -> 'PersBTreeNode':
        with self._lock:
            node_binary = node.to_binary()
            if len(node_binary) > BLOCK_SIZE_BYTES:
                raise PageOverflowException(f"Trying to {len(bytes)}, maximum page size is: {BLOCK_SIZE_BYTES}")
            binary_data = bytearray(BLOCK_SIZE_BYTES)
            binary_data[:len(node_binary)] = node_binary

            offset = self._seek_to_end()
            new_node_pointer = PagePointer(offset // BLOCK_SIZE_BYTES)
            # save to file only when cache space needs to be freed
            self._file.write(binary_data)
            # self._cache[new_node_pointer] = node
            node.pointer = new_node_pointer
        return node

    def _seek_to_end(self):
        return self._file.seek(0, os.SEEK_END)


class PageOverflowException(RuntimeError):
    def __init__(self, msg):
        super().__init__(msg)
