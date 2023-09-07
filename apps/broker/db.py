import os

from dataclasses import dataclass
from typing import List

from apps.broker.models import Record

FILE_NAME = 'db'
DB_FILE_HEADER_SIZE_BYTES = 1024
BLOCK_SIZE_BYTES = 1024
BLOCK_NUMBER_OF_SLOTS_SIZE_BYTES = 2  # max 65536 slots
SLOT_OFFSET_SIZE_BYTES = 2  # max 65536 offsets
SLOT_LENGTH_SIZE = 2  # max 65536 chars
SLOT_POINTER_SIZE_BYTES = SLOT_OFFSET_SIZE_BYTES + SLOT_LENGTH_SIZE
BLOCK_MAX_DATA_SIZE = (BLOCK_SIZE_BYTES - BLOCK_NUMBER_OF_SLOTS_SIZE_BYTES - SLOT_POINTER_SIZE_BYTES)
STR_ENCODING = 'utf8'
INT_ENCODING = 'big'


@dataclass
class DbSlotPointer:
    offset: int
    length: int

    def to_binary(self) -> bytes:
        return (int(self.offset).to_bytes(SLOT_OFFSET_SIZE_BYTES, INT_ENCODING) +
                int(self.length).to_bytes(SLOT_LENGTH_SIZE, INT_ENCODING))


@dataclass
class DbSlot:
    data: bytearray


@dataclass
class DbRecordIndex:
    block: int
    slot: int


@dataclass
class DbRecord:
    id: str
    data: str

    @classmethod
    def from_model(cls, record: Record):
        return cls(record.id, record.data)

    def to_model(self) -> Record:
        return Record(id=self.id, data=self.data)


class DbBlock:
    def __init__(self, block_number: int, slot_pointers: List[DbSlotPointer], data: bytearray):
        self.block_number = block_number
        self._slots = slot_pointers
        self._data = data

    def add_slot(self, slot_data: bytearray) -> DbRecordIndex:
        # update number of slots
        self._data[:BLOCK_NUMBER_OF_SLOTS_SIZE_BYTES] = int(len(self._slots) + 1).to_bytes(
            BLOCK_NUMBER_OF_SLOTS_SIZE_BYTES, INT_ENCODING)

        # update slot data
        if self._slots:
            first_offset = self._slots[-1].offset
            new_offset = first_offset - len(slot_data)
        else:
            new_offset = len(self._data) - len(slot_data)
        self._data[new_offset:new_offset + len(slot_data)] = slot_data

        # update slots pointers
        new_slot_pointer = DbSlotPointer(offset=new_offset, length=len(slot_data))
        slot_start_offset = BLOCK_NUMBER_OF_SLOTS_SIZE_BYTES + SLOT_POINTER_SIZE_BYTES * len(self._slots)
        slot_end_offset = slot_start_offset + SLOT_POINTER_SIZE_BYTES
        self._data[slot_start_offset: slot_end_offset] = new_slot_pointer.to_binary()
        self._slots.append(new_slot_pointer)
        return DbRecordIndex(self.block_number, len(self._slots) - 1)

    def has_space_for_data(self, data: bytes) -> bool:
        # len(self.slots) + 1 because we have to count for a new slot pointer too
        return len(data) <= (
            BLOCK_SIZE_BYTES - BLOCK_NUMBER_OF_SLOTS_SIZE_BYTES - (len(self._slots) + 1) * SLOT_POINTER_SIZE_BYTES)

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
            offset_slice = slice(i * SLOT_LENGTH_SIZE, i * SLOT_LENGTH_SIZE + SLOT_OFFSET_SIZE_BYTES)
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
        if working_block.block_number == 0:
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


class BrokerDb:
    def append_record(self, record: DbRecord) -> (int, int):
        binary_data = bytearray(record.data.encode(STR_ENCODING))
        if not DbBlock.data_fits_empty_block(binary_data):
            raise DataToLarge(f'Maximum data size is {BLOCK_MAX_DATA_SIZE}')

        with open(FILE_NAME, 'ab+') as file:
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

    def read_record(self, index: DbRecordIndex) -> DbRecord:
        return DbRecord('', '')

    @staticmethod
    def is_empty(file_name):
        return os.stat(file_name).st_size == 0


class DataToLarge(RuntimeError):
    def __init__(self, msg: str):
        super().__init__(msg)
