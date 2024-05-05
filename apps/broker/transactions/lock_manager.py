import threading
import typing

from apps.broker.transactions.transaction import TxnId
from apps.broker.transactions.record import DbKey


class RWLock:
    def __init__(self):
        self.lock = threading.Lock()
        self._condition = threading.Condition()
        self._read_txns: typing.Dict[TxnId, int] = {}
        self._write_txns: typing.Dict[TxnId, int] = {}

    def acquire_read(self, txn_id: TxnId):
        with self._condition:
            while not ((len(self._write_txns) == 1 and txn_id in self._write_txns) or
                       not self._write_txns):
                self._condition.wait()
            if txn_id not in self._read_txns:
                self._read_txns[txn_id] = 0
            self._read_txns[txn_id] += 1

    def release_read(self, txn_id: TxnId):
        with self._condition:
            if txn_id not in self._read_txns:
                raise LockNotAcquiredException(f"Read lock not hold by transaction {txn_id}")
            self._read_txns[txn_id] -= 1
            if self._read_txns[txn_id] == 0:
                del self._read_txns[txn_id]
            if not self._read_txns:
                self._condition.notify_all()

    def acquire_write(self, txn_id: TxnId):
        with self._condition:
            while not ((len(self._read_txns) == 1 and txn_id in self._read_txns) or
                       (len(self._write_txns) == 1 and txn_id in self._write_txns) or
                       (not self._read_txns and not self._write_txns)):
                self._condition.wait()
            if txn_id not in self._write_txns:
                self._write_txns[txn_id] = 0
            self._write_txns[txn_id] += 1

    def release_write(self, txn_id: TxnId):
        with self._condition:
            if txn_id not in self._write_txns:
                raise LockNotAcquiredException(f"Write lock not hold by current thread {txn_id}")
            self._write_txns[txn_id] -= 1
            if self._write_txns[txn_id] == 0:
                del self._write_txns[txn_id]
            self._condition.notify_all()


class LockManager:
    def __init__(self):
        self._locks: typing.Dict[DbKey, RWLock] = {}

    def get_rw_lock(self, key: DbKey):
        # TODO: make cleaning when lock not needed anymore
        if key not in self._locks:
            self._locks[key] = RWLock()
        return self._locks[key]


class LockNotAcquiredException(Exception):
    def __init__(self, msg: str) -> None:
        super().__init__(msg)
