import io
import os
import typing
from dataclasses import dataclass

from apps.broker.storage.storage_engine import DbRecord

INT_ENCODING = 'big'
MAX_RECORD_LENGTH_BYTES = 4  # 2^31 - 1
UTF8 = 'utf-8'

@dataclass(frozen=True)
class DbKey:
    key: str

    def to_str(self):
        return self.key


@dataclass(frozen=True)
class DbRecord:
    key: DbKey
    value: typing.Any

    def to_str(self):
        return f"{self.key.to_str()}={self.value}"

    @classmethod
    def from_str(cls, s: str) -> 'DbRecord':
        data = s.split("=")
        return DbRecord(DbKey(data[0]), data[1])


@dataclass(frozen=True)
class PersistedDbRecord:
    offset: int
    record: DbRecord

    def to_binary(self) -> io.BytesIO:
        record_bytes = self.record.to_str().encode(UTF8)
        length = int(len(record_bytes)).to_bytes(MAX_RECORD_LENGTH_BYTES, INT_ENCODING)
        buff = io.BytesIO()
        buff.write(length)
        buff.write(record_bytes)
        return buff

    @classmethod
    def from_binary(cls, offset: int, length: int, buff: io.BytesIO) -> 'PersistedDbRecord':
        return PersistedDbRecord(offset, DbRecord.from_str(buff.read(length).decode(UTF8)))


class DbRecordDoesNotExists(BaseException):
    def __init__(self, msg: str):
        super().__init__(msg)


class BufferPool:
    def __init__(self, file_path):
        _file_path = file_path
        self._create_file(_file_path)
        self._file = open(_file_path, 'r+b')
        self._end_offset = 0

    def append(self, record: DbRecord) -> PersistedDbRecord:
        start_offset = self._end_offset
        persisted = PersistedDbRecord(start_offset, record)
        data = persisted.to_binary().getvalue()
        self._file.write(data)
        self._end_offset += len(data)
        return persisted

    def mark_as_garbage(self, prev_persisted_record: PersistedDbRecord) -> None:
        pass

    # def read(self, param):
    #     pass

    def load_all(self):
        data_buff = io.BytesIO()
        offset = 0
        file_end = self._file.seek(0, os.SEEK_END)
        self._file.seek(0)
        index: typing.Dict[DbKey, PersistedDbRecord] = {}

        while offset < file_end:
            record_len = int.from_bytes(self._file.read(MAX_RECORD_LENGTH_BYTES), INT_ENCODING)
            data_buff.write(self._file.read(record_len))
            data_buff.seek(0)
            persisted_record = PersistedDbRecord.from_binary(offset, record_len, data_buff)
            data_buff.seek(0)
            index[persisted_record.record.key] = persisted_record
            offset += record_len + MAX_RECORD_LENGTH_BYTES
        return index

    def close(self):
        self._file.close()

    @staticmethod
    def _create_file(file_path):
        with open(file_path, 'a+') as _:
            pass


class Database:
    def __init__(self, file_path=None):
        if not file_path:
            file_path = os.path.join(os.path.dirname(__file__), "db.txt")
        self._buff_pool = BufferPool(file_path)
        self._index: typing.Dict[DbKey, PersistedDbRecord] = self._buff_pool.load_all()

    def insert(self, record: DbRecord) -> PersistedDbRecord:
        persisted_record = self._buff_pool.append(record)
        self._index[record.key] = persisted_record
        return persisted_record

    def update(self, record: DbRecord) -> PersistedDbRecord:
        if record.key not in self._index:
            raise DbRecordDoesNotExists(f'Record with key: {record.key} does not exist')
        prev_persisted_record = self._index[record.key]
        new_persisted_record = self._buff_pool.append(record)
        self._index[record.key] = new_persisted_record
        self._buff_pool.mark_as_garbage(prev_persisted_record)
        return new_persisted_record

    def read(self, key: DbKey) -> PersistedDbRecord:
        if key not in self._index:
            raise DbRecordDoesNotExists(f'Record with key: {key} does not exist')
        return self._index[key]

    def delete(self, key: DbKey) -> None:
        if key not in self._index:
            raise DbRecordDoesNotExists(f'Record with key: {key} does not exist')
        prev_persisted_record = self._index[key]
        del self._index[key]
        self._buff_pool.mark_as_garbage(prev_persisted_record)

    def close(self):
        self._buff_pool.close()
