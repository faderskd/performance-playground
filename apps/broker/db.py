import os

from apps.broker.models import Record


class DbRecord:
    def __init__(self, id: str, data: str):
        self.id = id
        self.data = data

    @classmethod
    def from_model(cls, record: Record):
        return cls(record.id, record.data)

    def to_model(self):
        return Record(id=self.id, data=self.data)


class BrokerDb:
    FILE_NAME = "db"
    BLOCK_SIZE = 1024
    BLOCK_SIZE_ENCODING_BYTES = 2
    ENCODING = 'utf8'

    def append_record(self, record: DbRecord) -> int:
        with open(self.FILE_NAME, 'ab+') as file:
            buffer = bytearray()
            encoded_data = record.data.encode(self.ENCODING)
            buffer.extend(bytes(int(len(encoded_data)).to_bytes(self.BLOCK_SIZE_ENCODING_BYTES, 'little')))
            buffer.extend(encoded_data)
            if self.BLOCK_SIZE > len(buffer):
                buffer.extend(bytes(b'0' * (self.BLOCK_SIZE - len(buffer))))
            file.write(buffer)
            return (file.tell() // self.BLOCK_SIZE) - 1

    def read_record(self, offset: int) -> DbRecord:
        with open(self.FILE_NAME, 'rb') as file:
            db_offset = offset * self.BLOCK_SIZE
            file.seek(db_offset)
            data_len = int.from_bytes(file.read(self.BLOCK_SIZE_ENCODING_BYTES), 'little')
            data = file.read(data_len).decode(self.ENCODING)
            return DbRecord('', data)

    @staticmethod
    def is_empty(file_name):
        return os.stat(file_name).st_size == 0
