import abc
import io
import os
import typing
from dataclasses import dataclass

from apps.broker.transactions.record import INT_ENCODING, MAX_RECORD_LENGTH_BYTES, DbKey, DbRecord, PersistedDbRecord, \
    DbRecordDoesNotExists


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


@dataclass(frozen=True)
class TxnId:
    id: int


class TxnIdGenerator:
    def __init__(self):
        self._txn_id = -1

    def generate(self) -> TxnId:
        self._txn_id += 1
        return TxnId(self._txn_id)


class TxnOp(abc.ABC):
    pass


class TxnInsertOp(TxnOp):
    def __init__(self, record: DbRecord):
        self.record = record


class TxnMetadata:
    def __init__(self):
        self._operations: typing.List[TxnOp] = []

    def add_operation(self, txn_op: TxnOp):
        self._operations.append(txn_op)

    @property
    def operations(self) -> typing.List[TxnOp]:
        return self._operations


class InvalidTransactionId(RuntimeError):
    def __init__(self, msg):
        super().__init__(msg)


class Database:
    def __init__(self, file_path=None):
        if not file_path:
            file_path = os.path.join(os.path.dirname(__file__), "db.txt")
        self._buff_pool = BufferPool(file_path)
        self._index: typing.Dict[DbKey, PersistedDbRecord] = self._buff_pool.load_all()
        self._txn_generator = TxnIdGenerator()
        self._pending_transactions: typing.Dict[TxnId, TxnMetadata] = dict()

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

    def begin_transaction(self) -> TxnId:
        txn_id = self._txn_generator.generate()
        self._pending_transactions[txn_id] = TxnMetadata()
        return txn_id

    def txt_insert(self, tx_id: TxnId, record: DbRecord) -> None:
        self._ensure_transaction_exists(tx_id)
        self._pending_transactions[tx_id].add_operation(TxnInsertOp(record))

    def txn_update(self, tx_id: TxnId, record: DbRecord) -> None:
        self.update(record)

    def txt_delete(self, tx_id: TxnId, key: DbKey) -> None:
        self.delete(key)

    def txt_read(self, tx_id: TxnId, key: DbKey) -> PersistedDbRecord:
        return self.read(key)

    def txn_abort(self, tx_id: TxnId) -> None:
        self._ensure_transaction_exists(tx_id)
        del self._pending_transactions[tx_id]

    def txn_commit(self, tx_id: TxnId) -> None:
        self._ensure_transaction_exists(tx_id)
        for op in self._pending_transactions[tx_id].operations:
            if isinstance(op, TxnInsertOp):
                self.insert(op.record)
        del self._pending_transactions[tx_id]

    def _ensure_transaction_exists(self, tx_id):
        if tx_id not in self._pending_transactions:
            raise InvalidTransactionId(f"There is no pending transaction for tx_id: {tx_id}")
