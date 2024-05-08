import enum
import threading
import typing
from collections import defaultdict

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
                return
            self._read_txns[txn_id] -= 1
            if self._read_txns[txn_id] == 0:
                del self._read_txns[txn_id]
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
                return
            self._write_txns[txn_id] -= 1
            if self._write_txns[txn_id] == 0:
                del self._write_txns[txn_id]
            self._condition.notify_all()


class OpType(enum.Enum):
    READ = 0
    WRITE = 1


class LockAcquisition:
    def __init__(self, rw_lock: RWLock, op: OpType, txn_id: TxnId):
        self.rw_lock = rw_lock
        self.op = op
        self.txn_id = txn_id

    def release(self):
        if self.op == OpType.READ:
            self.rw_lock.release_read(self.txn_id)
        else:
            self.rw_lock.release_write(self.txn_id)


class LockManager:
    def __init__(self):
        self._locks: typing.Dict[DbKey, RWLock] = {}
        self._txn_locks: typing.Dict[TxnId, typing.List[LockAcquisition]] = defaultdict(list)

    def read_lock(self, txn_id: TxnId, key: DbKey) -> RWLock:
        lock = self._get_rw_lock(key)
        self._txn_locks[txn_id].append(LockAcquisition(lock, OpType.READ, txn_id))
        return lock

    def write_lock(self, txn_id: TxnId, key: DbKey) -> RWLock:
        lock = self._get_rw_lock(key)
        self._txn_locks[txn_id].append(LockAcquisition(lock, OpType.WRITE, txn_id))
        return lock

    def _get_rw_lock(self, key: DbKey):
        # TODO: make cleaning when lock not needed anymore
        if key not in self._locks:
            self._locks[key] = RWLock()
        return self._locks[key]

    def release_locks_for_txn(self, txn_id) -> typing.List[LockAcquisition]:
        for lock in self._txn_locks[txn_id]:
            lock.release()

class LockNotAcquiredException(Exception):
    def __init__(self, msg: str) -> None:
        super().__init__(msg)
