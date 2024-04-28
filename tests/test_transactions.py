import abc
import logging
import typing
import unittest
from concurrent import futures
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import List
from unittest import TestCase

from apps.broker.transactions.database import Database
from apps.broker.transactions.transaction import TxnId
from apps.broker.transactions.record import DbKey, DbRecord, DbRecordDoesNotExists, DbRecordAlreadyExists
from tests.test_utils import random_string, ensure_file_not_exists_in_current_dir

logger = logging.getLogger(__name__)


class TxnOperation(abc.ABC):
    @abc.abstractmethod
    def execute(self, db: Database, tx: TxnId):
        pass


@dataclass
class TxnInsertOp(TxnOperation):
    record: DbRecord

    def execute(self, db: Database, txn_id: TxnId):
        db.txn_insert(txn_id, self.record)


@dataclass
class TxnUpdateOp(TxnOperation):
    record: DbRecord

    def execute(self, db: Database, txn_id: TxnId):
        db.txn_update(txn_id, self.record)


@dataclass
class TxnDeleteOp(TxnOperation):
    record: DbRecord

    def execute(self, db: Database, txn_id: TxnId):
        db.txn_delete(txn_id, self.record.key)


@dataclass
class TxnReadOp(TxnOperation):
    record: DbRecord

    def execute(self, db: Database, txn_id: TxnId):
        db.txn_read(txn_id, self.record.key)


@dataclass
class TxnIncrementIntOp(TxnOperation):
    record: DbRecord

    def execute(self, db: Database, txn_id: TxnId):
        current_val = int(db.txn_read(txn_id, self.record.key).value)
        current_val += 1
        new_record = DbRecord(self.record.key, current_val)
        db.txn_update(txn_id, new_record)


@dataclass
class TxnConcatStrOp(TxnOperation):
    record: DbRecord
    add_string: str

    def execute(self, db: Database, txn_id: TxnId):
        current_val = db.txn_read(txn_id, self.record.key).value
        current_val += self.add_string
        new_record = DbRecord(self.record.key, current_val)
        db.txn_update(txn_id, new_record)


class Transaction:
    def __init__(self):
        self._ops: typing.List[TxnOperation] = []

    def add(self, operation: TxnOperation):
        self._ops.append(operation)

    def execute(self, db: Database, abort=False):
        txn_id = db.begin_transaction()
        for op in self._ops:
            op.execute(db, txn_id)
        if abort:
            db.txn_abort(txn_id)
        else:
            db.txn_commit(txn_id)


def threaded_insert(chunk: List[DbRecord], db: Database) -> None:
    for rec in chunk:
        transaction = Transaction()
        transaction.add(TxnInsertOp(rec))
        transaction.execute(db)


# TODO, handle:
#   concurrent insert of the same key in transactions

class ThreadingUtilsMixin:
    def setUp(self) -> None:
        super().setUp()
        self._executor = futures.ThreadPoolExecutor(max_workers=5)

    def run_on_thread(self, fn: typing.Callable[[...], typing.Any]) -> typing.Callable[[...], futures.Future]:
        def wrapper(*args, **kwargs) -> futures.Future:
            return self._executor.submit(fn, *args, **kwargs)

        return wrapper

    @staticmethod
    def wait_for_all(*future_list: Future):
        return futures.wait([f for f in future_list])


class TestTransactionsEngine(ThreadingUtilsMixin, TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.test_db_file_path = ensure_file_not_exists_in_current_dir('db.txt')

    def test_should_see_committed_changes_for_insert(self):
        # given
        db = Database(self.test_db_file_path)
        record = DbRecord(DbKey("key"), "value")

        # when
        tx_id = db.begin_transaction()
        db.txn_insert(tx_id, record)
        db.txn_commit(tx_id)

        # then
        self.assertEqual(db.read(DbKey("key")), record)

    def test_should_see_committed_changes_for_delete(self):
        # given
        db = Database(self.test_db_file_path)
        record = DbRecord(DbKey("key"), "value")
        db.insert(record)
        self.assertEqual(db.read(record.key), record)

        # when
        tx_id = db.begin_transaction()
        db.txn_delete(tx_id, record.key)
        db.txn_commit(tx_id)

        # then
        with self.assertRaises(DbRecordDoesNotExists):
            db.read(record.key)

    def test_should_see_only_committed_changes_for_update(self):
        # given
        db = Database(self.test_db_file_path)
        before_update = DbRecord(DbKey("key"), "value")
        db.insert(before_update)

        # when
        updated = DbRecord(DbKey("key"), "updated")
        txn_id = db.begin_transaction()
        db.txn_update(txn_id, updated)
        db.txn_commit(txn_id)

        # then
        self.assertEqual(db.read(DbKey("key")), updated)

    def test_should_not_see_aborted_changes_for_insert(self):
        # given
        db = Database(self.test_db_file_path)
        record = DbRecord(DbKey("key"), "value")

        # when
        tx_id = db.begin_transaction()
        db.txn_insert(tx_id, record)
        db.txn_abort(tx_id)

        # then
        with self.assertRaises(DbRecordDoesNotExists):
            db.read(DbKey("key"))

    def test_should_not_see_aborted_changes_for_update(self):
        # given
        db = Database(self.test_db_file_path)
        before_update = DbRecord(DbKey("key"), "value")
        db.insert(before_update)

        # when
        updated = DbRecord(DbKey("key"), "updated")
        txn_id = db.begin_transaction()
        db.txn_update(txn_id, updated)
        db.txn_abort(txn_id)

        # then
        self.assertEqual(db.read(DbKey("key")), before_update)

    def test_should_not_see_aborted_changes_for_delete(self):
        # given
        db = Database(self.test_db_file_path)
        record = DbRecord(DbKey("key"), "value")
        db.insert(record)
        self.assertEqual(db.read(record.key), record)

        # when
        tx_id = db.begin_transaction()
        db.txn_delete(tx_id, record.key)
        db.txn_abort(tx_id)

        # then
        self.assertEqual(db.read(record.key), record)

    def test_should_not_block_two_transactions_trying_to_read_the_same_record(self):
        # given
        db = Database(self.test_db_file_path)
        record = DbRecord(DbKey("key"), "value")
        db.insert(record)

        txn_id_1 = db.begin_transaction()
        txn_id_2 = db.begin_transaction()

        # when
        @self.run_on_thread
        def reader_1() -> DbRecord:
            return db.txn_read(txn_id_1, record.key)

        @self.run_on_thread
        def reader_2() -> DbRecord:
            return db.txn_read(txn_id_2, record.key)

        future_1 = reader_1()
        future_2 = reader_2()

        # then
        self.assertEqual(future_1.result(), future_2.result())

        # cleanup
        db.txn_abort(txn_id_1)
        db.txn_abort(txn_id_2)

    def test_should_block_writer_transaction_writing_to_already_read_record(self):
        # given
        db = Database(self.test_db_file_path)
        record = DbRecord(DbKey("key"), "value")
        db.insert(record)

        txn_id_1 = db.begin_transaction()
        txn_id_2 = db.begin_transaction()

        # when
        db.txn_read(txn_id_1, record.key)

        @self.run_on_thread
        def writer() -> None:
            return db.txn_update(txn_id_2, record)

        with self.assertRaises(futures.TimeoutError):
            writer().result(timeout=0.01)

        # cleanup
        db.txn_abort(txn_id_1)
        db.txn_abort(txn_id_2)

    def test_should_block_writer_transaction_writing_to_already_written_record(self):
        # given
        db = Database(self.test_db_file_path)
        record = DbRecord(DbKey("key"), "value")
        db.insert(record)

        txn_id_1 = db.begin_transaction()
        txn_id_2 = db.begin_transaction()

        # when
        db.txn_update(txn_id_1, record)

        @self.run_on_thread
        def writer() -> None:
            return db.txn_update(txn_id_2, record)

        with self.assertRaises(futures.TimeoutError):
            writer().result(timeout=0.01)

        # cleanup
        db.txn_abort(txn_id_1)
        db.txn_abort(txn_id_2)

    def test_should_block_reader_transaction_reading_already_written_record(self):
        # given
        db = Database(self.test_db_file_path)
        record = DbRecord(DbKey("key"), "value")
        db.insert(record)

        txn_id_1 = db.begin_transaction()
        txn_id_2 = db.begin_transaction()

        # when
        db.txn_update(txn_id_1, record)

        @self.run_on_thread
        def reader() -> DbRecord:
            return db.txn_read(txn_id_2, record.key)

        with self.assertRaises(futures.TimeoutError):
            reader().result(timeout=0.01)

        # cleanup
        db.txn_abort(txn_id_1)
        db.txn_abort(txn_id_2)

    def test_should_see_all_transaction_operations_locally(self):
        # given
        db = Database(self.test_db_file_path)
        record = DbRecord(DbKey("key"), "value")
        updated = DbRecord(DbKey("key"), "updated")

        # when
        txn_id = db.begin_transaction()
        db.txn_insert(txn_id, record)
        record_in_txn = db.txn_read(txn_id, record.key)

        # then
        self.assertEqual(record_in_txn, record)

        # when
        db.txn_update(txn_id, updated)

        # then
        record_in_txn = db.txn_read(txn_id, record.key)
        self.assertEqual(record_in_txn, updated)

        # when
        db.txn_delete(txn_id, record.key)

        # then
        with self.assertRaises(DbRecordDoesNotExists):
            db.txn_read(txn_id, record.key)

    def test_should_raise_error_when_updating_not_existing_record(self):
        # given
        db = Database(self.test_db_file_path)

        # when
        txn_id = db.begin_transaction()

        # then
        with self.assertRaises(DbRecordDoesNotExists):
            db.txn_update(txn_id, DbRecord(DbKey("key"), "value"))

    def test_should_raise_error_when_deleting_not_existing_record(self):
        # given
        db = Database(self.test_db_file_path)

        # when
        txn_id = db.begin_transaction()

        # then
        with self.assertRaises(DbRecordDoesNotExists):
            db.txn_delete(txn_id, DbKey("key"))

    def test_should_raise_error_when_updating_locally_deleted_element(self):
        # given
        db = Database(self.test_db_file_path)
        record = DbRecord(DbKey("key"), "value")
        db.insert(record)

        # when
        txn_id = db.begin_transaction()
        db.txn_delete(txn_id, record.key)

        # then
        with self.assertRaises(DbRecordDoesNotExists):
            db.txn_update(txn_id, DbRecord(DbKey("key"), "updated"))

    def test_should_raise_error_when_reading_locally_deleted_element(self):
        # given
        db = Database(self.test_db_file_path)
        record = DbRecord(DbKey("key"), "value")
        db.insert(record)

        # when
        txn_id = db.begin_transaction()
        db.txn_delete(txn_id, record.key)

        # then
        with self.assertRaises(DbRecordDoesNotExists):
            db.txn_read(txn_id, record.key)

    def test_should_raise_error_when_inserting_already_existing_element(self):
        # given
        db = Database(self.test_db_file_path)
        record = DbRecord(DbKey("key"), "value")
        db.insert(record)

        # when
        txn_id = db.begin_transaction()

        # then
        with self.assertRaises(DbRecordAlreadyExists):
            db.txn_insert(txn_id, record)

    def test_should_detect_deadlock(self):
        """
        T1      |   T2
        read A  |   read A
        write A |   write A
        """
        # given
        A_key = DbKey("A")
        db = Database(self.test_db_file_path)
        db.insert(DbRecord(A_key, 1))

        # when
        t1 = db.begin_transaction()
        t2 = db.begin_transaction()

        db.txn_read(t1, A_key)
        db.txn_read(t2, A_key)

        db.txn_update(t1, DbRecord(A_key, 2))

    @unittest.skip("activate when detecting deadlock")
    def test_should_serialize_two_concurrent_transactions(self):
        """
        A = 1000
        B = 1000

        T1            |    T2
        BEGIN         |    BEGIN
        A = A - 100   |    A = A * 1.2
        B = B + 100   |    B = B * 1.2
        COMMIT        |    COMMIT
                      |
        RESULT        |    RESULT
        A + B = 2000  |    2000 * 1.2 = 2400

        """
        # given
        A_key = DbKey("A")
        B_key = DbKey("B")
        db = Database(self.test_db_file_path)
        db.insert(DbRecord(A_key, 1000))
        db.insert(DbRecord(B_key, 1000))

        # when
        t1 = db.begin_transaction()
        t2 = db.begin_transaction()

        a_t1 = db.txn_read(t1, A_key)
        b_t1 = db.txn_read(t1, B_key)

        a_t2 = db.txn_read(t2, A_key)
        b_t2 = db.txn_read(t2, B_key)

        db.txn_update(t1, DbRecord(A_key, a_t1.value - 100))
        db.txn_update(t2, DbRecord(A_key, a_t2.value * 1.2))
        db.txn_update(t1, DbRecord(B_key, b_t1.value + 100))
        db.txn_update(t2, DbRecord(B_key, b_t2.value * 1.2))

        db.txn_commit(t1)
        db.txn_commit(t2)

        # then
        self.assertEqual(db.read(A_key).value, 1080)  # 1.2 * 900
        self.assertEqual(db.read(B_key).value, 1320)  # 1.2 * 1100

    @unittest.skip("This is for MVCC")
    def test_should_read_local_data_from_transaction(self):
        # given
        db = Database(self.test_db_file_path)
        before_update = DbRecord(DbKey("key"), "value")
        db.insert(before_update)

        updated = DbRecord(DbKey("key"), "updated")
        txn_id = db.begin_transaction()
        db.txn_update(txn_id, updated)

        # when
        record_in_txn = db.txn_read(txn_id, DbKey("key"))
        global_record = db.read(DbKey("key"))

        # then
        self.assertEqual(record_in_txn, updated)
        self.assertEqual(global_record, before_update)

    @unittest.skip("This will be separated test")
    def test_should_handle_concurrent_transactions(self):
        # given
        db = Database(self.test_db_file_path)
        existing_records = [i for i in range(10000)]
        records_to_insert = [DbRecord(DbKey(f"id{i}"), f"val{i}") for i in
                             range(len(existing_records), int(1.25 * len(existing_records)))]
        insert_threads = 5
        thread_pool = ThreadPoolExecutor(max_workers=insert_threads, thread_name_prefix="test-transactions")

        # for rec in existing_records:
        #     db.insert(DbRecord(DbKey(str(rec)), str(rec)))

        insert_chunks = divide_into_chunks(records_to_insert, insert_threads)

        # when
        insert_futures = [thread_pool.submit(threaded_insert, chunk, db) for chunk in insert_chunks]
        for f in insert_futures:
            f.result()

        # then
        for rec in records_to_insert:
            self.assertEqual(db.read(rec.key).value, rec.value)


def divide_into_chunks(records: List[typing.Any], chunks: int) -> List[List[typing.Any]]:
    chunk_size = len(records) // chunks
    result = [records[i * chunk_size: (i + 1) * chunk_size] for i in range(chunks - 1)]
    result.append(records[(chunks - 1) * chunk_size:])
    return result
