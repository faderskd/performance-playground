import io
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


class DbRecordAlreadyExists(BaseException):
    def __init__(self, msg: str):
        super().__init__(msg)
