import io
from dataclasses import dataclass

PAGE_POINTER_BLOCK_NUMBER_BYTES = 4  # max 4294967296 pages in a tree
PAGE_KEY_BYTES = 6  # max 2^48 elements
INT_ENCODING = 'big'


@dataclass(frozen=True)
class PagePointer:
    block_number: int

    def to_binary(self) -> bytes:
        return int(self.block_number).to_bytes(PAGE_POINTER_BLOCK_NUMBER_BYTES, INT_ENCODING, signed=True)

    @staticmethod
    def binary_none() -> bytes:
        return int(-1).to_bytes(PAGE_POINTER_BLOCK_NUMBER_BYTES, INT_ENCODING, signed=True)

    @classmethod
    def from_binary(cls, data: io.BytesIO) -> 'PagePointer':
        pointer = int.from_bytes(data.read(PAGE_POINTER_BLOCK_NUMBER_BYTES), INT_ENCODING, signed=True)
        if pointer >= 0:
            return PagePointer(pointer)


@dataclass
class PersKey:
    key: int

    def to_binary(self) -> bytes:
        return int(self.key).to_bytes(PAGE_KEY_BYTES, INT_ENCODING)

    @classmethod
    def from_binary(cls, data: io.BytesIO) -> 'PersKey':
        return cls(int.from_bytes(data.read(PAGE_KEY_BYTES), INT_ENCODING))

    def __gt__(self, other):
        return self.key > other.key

    def __ge__(self, other):
        return self.key >= other.key

    def __repr__(self):
        return str(self.key)
