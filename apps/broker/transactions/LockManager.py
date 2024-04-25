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
            while txn_id not in self._read_txns and txn_id not in self._write_txns and self._write_txns:
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
            while txn_id not in self._read_txns and txn_id not in self._write_txns and (
                self._write_txns or self._read_txns):
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

# lock = RWLock()
#
#
# def read():
#     lock.acquire_read()
#     for i in range(5):
#         sleep(1)
#         print(f"read: {current_thread()} -> {i}")
#     lock.release_read()
#
#
# def write():
#     lock.acquire_write()
#     for i in range(5):
#         sleep(1)
#         print(f"write: {current_thread()} -> {i}")
#     lock.release_write()
#
#
# r_thread1 = threading.Thread(target=read)
# r_thread2 = threading.Thread(target=read)
# w_thread = threading.Thread(target=write)
#
# r_thread1.start()
# r_thread2.start()
# w_thread.start()
#
# r_thread1.join()
# r_thread2.join()
# w_thread.join()
