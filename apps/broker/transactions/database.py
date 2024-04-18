import io
import os
import threading
import typing

from apps.broker.transactions.LockManager import LockManager
from apps.broker.transactions.record import INT_ENCODING, MAX_RECORD_LENGTH_BYTES, DbKey, DbRecord, PersistedDbRecord, \
    DbRecordDoesNotExists, DbRecordAlreadyExists
from apps.broker.transactions.transaction import TxnId, TxnIdGenerator, InvalidTransactionId
from apps.broker.transactions.transaction_metatada import TxnInsertOp, TxnUpdateOp, TxnReadOp, TxnMetadata


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
        self._txn_generator = TxnIdGenerator()
        self._pending_transactions: typing.Dict[TxnId, TxnMetadata] = dict()
        self._lock_manager: LockManager = LockManager()
        self._internal_lock = threading.Lock()

    def insert(self, record: DbRecord):
        txn_id = self.begin_transaction()
        self.txn_insert(txn_id, record)
        self.txn_commit(txn_id)

    def update(self, record: DbRecord) -> None:
        txn_id = self.begin_transaction()
        self.txn_update(txn_id, record)
        self.txn_commit(txn_id)

    def read(self, key: DbKey) -> DbRecord:
        if key not in self._index:
            raise DbRecordDoesNotExists(f'Record with key: {key} does not exist')
        return self._index[key].record

    def delete(self, key: DbKey) -> None:
        if key not in self._index:
            raise DbRecordDoesNotExists(f'Record with key: {key} does not exist')
        prev_persisted_record = self._index[key]
        del self._index[key]
        self._buff_pool.mark_as_garbage(prev_persisted_record)

    def close(self):
        self._buff_pool.close()

    def begin_transaction(self) -> TxnId:
        with self._internal_lock:
            txn_id = self._txn_generator.generate()
            self._pending_transactions[txn_id] = TxnMetadata()
            return txn_id

    def txn_insert(self, txn_id: TxnId, record: DbRecord) -> None:
        with self._internal_lock:  # TODO: is it necessary ?
            self._ensure_transaction_exists(txn_id)
            self._pending_transactions[txn_id].add_operation(TxnInsertOp(record, txn_id, lock=None))

    def txn_update(self, txn_id: TxnId, record: DbRecord) -> None:
        with self._internal_lock:
            self._ensure_transaction_exists(txn_id)
            lock = self._lock_manager.get_rw_lock(record.key)
        # can block, so we have to do it outside of internal lock
        lock.acquire_write(txn_id)
        with self._internal_lock:
            self._pending_transactions[txn_id].add_operation(TxnUpdateOp(record, txn_id, lock))

    def txn_delete(self, txn_id: TxnId, key: DbKey) -> None:
        self.delete(key)

    def txn_read(self, txn_id: TxnId, key: DbKey) -> DbRecord:
        self._ensure_transaction_exists(txn_id)
        with self._internal_lock:
            lock = self._lock_manager.get_rw_lock(key)
        # can block, so we have to do it outside of internal lock
        lock.acquire_read(txn_id)
        try:
            existing = self._index[key].record
        except KeyError:
            lock.release_read(txn_id)
            raise DbRecordDoesNotExists(f'Record with key: {key} does not exist')
        with self._internal_lock:
            return self._pending_transactions[txn_id].add_operation(TxnReadOp(existing, txn_id, lock))

    def txn_abort(self, txn_id: TxnId) -> None:
        self._ensure_transaction_exists(txn_id)
        for op in self._pending_transactions[txn_id].operations:
            op.release_lock()
        del self._pending_transactions[txn_id]

    def txn_commit(self, txn_id: TxnId) -> None:
        self._ensure_transaction_exists(txn_id)
        for op in self._pending_transactions[txn_id].operations:
            if isinstance(op, TxnInsertOp):
                self._insert(op.record)
            if isinstance(op, TxnUpdateOp):
                self._update(op.record)
        for op in self._pending_transactions[txn_id].operations:
            op.release_lock()
        del self._pending_transactions[txn_id]

    def _ensure_transaction_exists(self, txn_id):
        if txn_id not in self._pending_transactions:
            raise InvalidTransactionId(f"There is no pending transaction for tx_id: {txn_id}")

    def _insert(self, record: DbRecord) -> PersistedDbRecord:
        persisted_record = self._buff_pool.append(record)
        self._index[record.key] = persisted_record
        return persisted_record

    def _update(self, record: DbRecord) -> PersistedDbRecord:
        prev_persisted_record = self._index[record.key]
        new_persisted_record = self._buff_pool.append(record)
        self._index[record.key] = new_persisted_record
        self._buff_pool.mark_as_garbage(prev_persisted_record)
        return new_persisted_record

    def _ensure_record_does_not_exist(self, record):
        if record.key in self._index:
            raise DbRecordAlreadyExists(f'Record with key: {record.key} already exists')

    def _ensure_record_exists(self, key: DbKey):
        if key not in self._index:
            raise DbRecordDoesNotExists(f'Record with key: {key} does not exist')

# TODO:
#   1. Make index concurrent without a single lock ?
