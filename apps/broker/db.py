import os
from collections import namedtuple
from dataclasses import dataclass
from typing import List

from apps.broker.models import Record

FILE_NAME = 'db'
DB_FILE_HEADER_SIZE_BYTES = 1024
DB_FILE_NUMBER_OF_BLOCKS_SIZE_BYTES = 2  # max 65536 blocks
BLOCK_SIZE_BYTES = 1024
BLOCK_NUMBER_OF_SLOTS_SIZE_BYTES = 2  # max 65536 slots
SLOT_OFFSET_SIZE_BYTES = 2  # max 65536 offsets
SLOT_SIZE = 2  # max 65536 chars
SLOT_POINTER_SIZE_BYTES = SLOT_OFFSET_SIZE_BYTES + SLOT_SIZE
STR_ENCODING = 'utf8'
INT_ENCODING = 'big'


class DbRecord:
    def __init__(self, id: str, data: str):
        self.id = id
        self.data = data

    @classmethod
    def from_model(cls, record: Record):
        return cls(record.id, record.data)

    def to_model(self):
        return Record(id=self.id, data=self.data)


@dataclass
class DbSlotPointer:
    offset: int
    length: int


@dataclass
class DbSlot:
    data: str


class DbBlock:
    def __init__(self, slots: List[DbSlot]):
        self.slots = slots

    @classmethod
    def from_binary(cls, binary_block: bytes):
        number_of_slots = int.from_bytes(binary_block[:BLOCK_NUMBER_OF_SLOTS_SIZE_BYTES], INT_ENCODING)
        slots_pointers_slice = slice(BLOCK_NUMBER_OF_SLOTS_SIZE_BYTES,
                                     BLOCK_NUMBER_OF_SLOTS_SIZE_BYTES + SLOT_POINTER_SIZE_BYTES * number_of_slots)
        binary_slots_pointers = binary_block[slots_pointers_slice]
        slot_pointers = []
        for i in range(number_of_slots):
            offset_slice = slice(i * SLOT_SIZE, i * SLOT_SIZE + SLOT_OFFSET_SIZE_BYTES)
            length_slice = slice(i * SLOT_SIZE + SLOT_OFFSET_SIZE_BYTES, i * SLOT_SIZE + SLOT_SIZE)
            slot_pointers.append(
                DbSlotPointer(
                    offset=int.from_bytes(binary_slots_pointers[offset_slice], INT_ENCODING),
                    length=int.from_bytes(binary_slots_pointers[length_slice], INT_ENCODING)
                ))
        slots = []
        for sp in slot_pointers:
            slots.append(DbSlot(binary_block[sp.offset: sp.offset + sp.length].decode(STR_ENCODING)))
        return cls(slots)

    def has_space_for_data(self, data: str):
        pass


class BrokerDb:
    def append_record(self, record: DbRecord) -> int:
        with open(FILE_NAME, 'ab+') as file:
            binary_file_header = file.read(DB_FILE_HEADER_SIZE_BYTES)
            number_of_blocks = int.from_bytes(binary_file_header[:DB_FILE_NUMBER_OF_BLOCKS_SIZE_BYTES], INT_ENCODING)
            # + 1 because should add header block
            file.seek((number_of_blocks + 1) * BLOCK_SIZE_BYTES)
            binary_block = file.read(BLOCK_SIZE_BYTES)
            working_block = DbBlock.from_binary(binary_block)

            binary_file_header = bytearray()
            encoded_data = record.data.encode(STR_ENCODING)
            binary_file_header.extend(bytes(int(len(encoded_data)).to_bytes(BLOCK_SIZE_BYTES, INT_ENCODING)))
            binary_file_header.extend(encoded_data)
            if BLOCK_SIZE > len(binary_file_header):
                binary_file_header.extend(bytes(b'0' * (BLOCK_SIZE - len(binary_file_header))))
            file.write(binary_file_header)
            return (file.tell() // BLOCK_SIZE) - 1

    def read_record(self, offset: int) -> DbRecord:
        with open(FILE_NAME, 'rb') as file:
            db_offset = offset * BLOCK_SIZE
            file.seek(db_offset)
            buffer = file.read(BLOCK_SIZE)
            data_len = int.from_bytes(buffer[:BLOCK_HEADER_SIZE], 'big')
            data = buffer[
                   BLOCK_HEADER_SIZE:BLOCK_HEADER_SIZE + data_len].decode(STR_ENCODING)
            return DbRecord('', data)

    @staticmethod
    def is_empty(file_name):
        return os.stat(file_name).st_size == 0
