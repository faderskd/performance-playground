import os

from dataclasses import dataclass
from typing import List

from apps.broker.models import Record
from apps.broker.utils import public, private

DB_FILE_HEADER_SIZE_BYTES = 1024
BLOCK_SIZE_BYTES = 1024
BLOCK_NUMBER_OF_SLOTS_SIZE_BYTES = 2  # max 65536 slots
SLOT_OFFSET_SIZE_BYTES = 2  # max 65536 offsets
SLOT_LENGTH_SIZE = 2  # max 65536 chars
SLOT_POINTER_SIZE_BYTES = SLOT_OFFSET_SIZE_BYTES + SLOT_LENGTH_SIZE
BLOCK_MAX_DATA_SIZE = (BLOCK_SIZE_BYTES - BLOCK_NUMBER_OF_SLOTS_SIZE_BYTES - SLOT_POINTER_SIZE_BYTES)
STR_ENCODING = 'utf8'
INT_ENCODING = 'big'


@private
@dataclass
class DbSlotPointer:
    offset: int
    length: int

    def to_binary(self) -> bytes:
        return (int(self.offset).to_bytes(SLOT_OFFSET_SIZE_BYTES, INT_ENCODING) +
                int(self.length).to_bytes(SLOT_LENGTH_SIZE, INT_ENCODING))


@private
@dataclass
class DbSlot:
    data: bytearray


@private
@dataclass
class DbRecord:
    id: str
    data: str

    @classmethod
    def from_model(cls, record: Record):
        return cls(record.id, record.data)

    def to_model(self) -> Record:
        return Record(id=self.id, data=self.data)


@public
@dataclass
class DbRecordPointer:
    block: int
    slot: int


@private
class DbBlock:
    def __init__(self, block_number: int, slot_pointers: List[DbSlotPointer], data: bytearray):
        self.block_number = block_number
        self._slot_pointers = slot_pointers
        self._data = data

    def add_slot(self, slot_data: bytearray) -> DbRecordPointer:
        # update number of slots
        self._data[:BLOCK_NUMBER_OF_SLOTS_SIZE_BYTES] = int(len(self._slot_pointers) + 1).to_bytes(
            BLOCK_NUMBER_OF_SLOTS_SIZE_BYTES, INT_ENCODING)

        # update slot data
        if self._slot_pointers:
            first_offset = self._slot_pointers[-1].offset
            new_offset = first_offset - len(slot_data)
        else:
            new_offset = len(self._data) - len(slot_data)
        self._data[new_offset:new_offset + len(slot_data)] = slot_data

        # update slots pointers
        new_slot_pointer = DbSlotPointer(offset=new_offset, length=len(slot_data))
        slot_pointer_start_offset = BLOCK_NUMBER_OF_SLOTS_SIZE_BYTES + SLOT_POINTER_SIZE_BYTES * len(
            self._slot_pointers)
        slot_pointer_end_offset = slot_pointer_start_offset + SLOT_POINTER_SIZE_BYTES
        self._data[slot_pointer_start_offset: slot_pointer_end_offset] = new_slot_pointer.to_binary()
        self._slot_pointers.append(new_slot_pointer)
        return DbRecordPointer(self.block_number, len(self._slot_pointers) - 1)

    def has_space_for_data(self, data: bytes) -> bool:
        last_slot_pointer_offset = BLOCK_NUMBER_OF_SLOTS_SIZE_BYTES + len(self._slot_pointers)  * SLOT_POINTER_SIZE_BYTES
        if self._slot_pointers:
            first_data_pointer_offset = self._slot_pointers[-1].offset
        else:
            first_data_pointer_offset = BLOCK_SIZE_BYTES
        return len(data) + SLOT_POINTER_SIZE_BYTES <= first_data_pointer_offset - last_slot_pointer_offset

    @classmethod
    def empty(cls, block_number: int):
        return cls(block_number=block_number, slot_pointers=[], data=bytearray(BLOCK_SIZE_BYTES))

    @classmethod
    def from_binary(cls, block_number: int, binary_block: bytearray):
        number_of_slots = int.from_bytes(binary_block[:BLOCK_NUMBER_OF_SLOTS_SIZE_BYTES], INT_ENCODING)
        slots_pointers_slice = slice(BLOCK_NUMBER_OF_SLOTS_SIZE_BYTES,
                                     BLOCK_NUMBER_OF_SLOTS_SIZE_BYTES + SLOT_POINTER_SIZE_BYTES * number_of_slots)
        binary_slots_pointers = binary_block[slots_pointers_slice]
        slot_pointers = []
        for i in range(number_of_slots):
            offset_slice = slice(i * SLOT_POINTER_SIZE_BYTES, i * SLOT_POINTER_SIZE_BYTES + SLOT_OFFSET_SIZE_BYTES)
            length_slice = slice(offset_slice.stop, offset_slice.stop + SLOT_LENGTH_SIZE)
            slot_pointers.append(
                DbSlotPointer(
                    offset=int.from_bytes(binary_slots_pointers[offset_slice], INT_ENCODING),
                    length=int.from_bytes(binary_slots_pointers[length_slice], INT_ENCODING)
                ))
        return cls(block_number, slot_pointers, binary_block)

    @staticmethod
    def data_fits_empty_block(data: bytes):
        return len(data) <= BLOCK_MAX_DATA_SIZE

    def to_binary(self) -> bytes:
        return self._data

    def get_data(self, slot_number: int) -> bytes:
        if slot_number > len(self._slot_pointers):
            InvalidSlotExeption(f'Slot {slot_number} cannot be find in block {self.block_number}')
        offset = self._slot_pointers[slot_number].offset
        return self._data[offset:offset + self._slot_pointers[slot_number].length]


@private
class HeapFile:
    def __init__(self, file_handle):
        self._file = file_handle
        self._data_blocks = self.number_of_data_blocks()

    def get_working_block(self) -> DbBlock:
        if self._data_blocks == 0:
            return DbBlock.empty(0)

        block_number = self._data_blocks - 1
        self._file.seek(DB_FILE_HEADER_SIZE_BYTES + block_number * BLOCK_SIZE_BYTES)
        binary_block = bytearray(self._file.read(BLOCK_SIZE_BYTES))
        return DbBlock.from_binary(block_number, binary_block)

    def save(self, working_block: DbBlock):
        if self._data_blocks == 0:
            data_to_save = bytearray(DB_FILE_HEADER_SIZE_BYTES)
            data_to_save.extend(working_block.to_binary())
        else:
            data_to_save = working_block.to_binary()
            self._file.seek(DB_FILE_HEADER_SIZE_BYTES + working_block.block_number * BLOCK_SIZE_BYTES)
        self._file.write(data_to_save)

    def number_of_data_blocks(self) -> int:
        last_offset = self._file.seek(0, os.SEEK_END)
        if not last_offset:
            return 0
        return (last_offset - DB_FILE_HEADER_SIZE_BYTES) // BLOCK_SIZE_BYTES

    def get_block(self, index: DbRecordPointer) -> DbBlock:
        block_offset = DB_FILE_HEADER_SIZE_BYTES + index.block * BLOCK_SIZE_BYTES
        if self._file.seek(0, os.SEEK_END) < block_offset - BLOCK_SIZE_BYTES:
            raise InvalidOffsetExeption(f'Index {index} produces invalid offset while searching database block')
        self._file.seek(block_offset)
        binary_block = bytearray(self._file.read(BLOCK_SIZE_BYTES))
        return DbBlock.from_binary(index.block, binary_block)


@public
class DbEngine:
    def __init__(self, heap_file_path: str):
        self._db_file_path = heap_file_path
        self._create_heap_file(self._db_file_path)

    def append_record(self, record: DbRecord) -> (int, int):
        binary_data = bytearray(record.data.encode(STR_ENCODING))
        if not DbBlock.data_fits_empty_block(binary_data):
            raise DataToLargeException(f'Maximum data size is {BLOCK_MAX_DATA_SIZE}')

        with open(self._db_file_path, 'r+b') as file:
            heap_file = HeapFile(file)
            working_block = heap_file.get_working_block()

            if working_block.has_space_for_data(binary_data):
                index = working_block.add_slot(binary_data)
                heap_file.save(working_block)
            else:
                working_block = DbBlock.empty(heap_file.number_of_data_blocks())
                index = working_block.add_slot(binary_data)
                heap_file.save(working_block)
            return index

    def read_record(self, index: DbRecordPointer) -> DbRecord:
        with open(self._db_file_path, 'rb') as file:
            heap_file = HeapFile(file)
            working_block = heap_file.get_block(index)
            binary_data = working_block.get_data(index.slot)
            return DbRecord('', binary_data.decode(STR_ENCODING))

    def _create_heap_file(self, file_path):
        with open(file_path, 'a+') as _:
            pass


class DataToLargeException(RuntimeError):
    def __init__(self, msg: str):
        super().__init__(msg)


class InvalidOffsetExeption(ValueError):
    def __init__(self, msg: str):
        super().__init__(msg)


class InvalidSlotExeption(ValueError):
    def __init__(self, msg: str):
        super().__init__(msg)
