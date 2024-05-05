import abc
import typing

from apps.broker.transactions.LockManager import RWLock
from apps.broker.transactions.record import DbRecord, DbKey
from apps.broker.transactions.transaction import TxnId


class TxnOp(abc.ABC):
    def __init__(self, record: DbRecord, txn_id: TxnId, lock: typing.Optional[RWLock]):
        self.record = record
        self.txn_id = txn_id
        self._lock = lock

    @abc.abstractmethod
    def release_lock(self):
        pass


class TxnInsertOp(TxnOp):
    def release_lock(self):
        self._lock.release_write(self.txn_id)


class TxnUpdateOp(TxnOp):
    def release_lock(self):
        self._lock.release_write(self.txn_id)


class TxnReadOp(TxnOp):
    def release_lock(self):
        self._lock.release_read(self.txn_id)


class TxnDeleteOp(TxnOp):
    def release_lock(self):
        self._lock.release_write(self.txn_id)


class TxnMetadata:
    def __init__(self):
        self._operations: typing.List[TxnOp] = []
        self._local_index: typing.Dict[DbKey, DbRecord] = {}

    def add_operation(self, txn_op: TxnOp) -> None:
        self._operations.append(txn_op)
        if isinstance(txn_op, TxnInsertOp):
            self._local_index[txn_op.record.key] = txn_op.record
        elif isinstance(txn_op, TxnUpdateOp):
            self._local_index[txn_op.record.key] = txn_op.record
        elif isinstance(txn_op, TxnReadOp):
            pass
        elif isinstance(txn_op, TxnDeleteOp):
            self._local_index[txn_op.record.key] = DbRecord.tombstone(txn_op.record.key)

    @property
    def operations(self) -> typing.List[TxnOp]:
        return self._operations

    def contains_active_key(self, key: DbKey) -> bool:
        return not self._local_index.get(key, DbRecord.tombstone(key)).is_tombstone()

    def get_or_none(self, key: DbKey) -> typing.Optional[DbRecord]:
        return self._local_index.get(key, None)

    def txn_keys(self) -> typing.Set[DbKey]:
        return {op.record.key for op in self._operations}
