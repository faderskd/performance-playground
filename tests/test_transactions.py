import abc
import logging
import random
import typing
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import List
from unittest import TestCase

from apps.broker.transactions.database import Database, DbKey, DbRecord, TxId
from tests.test_utils import random_string, ensure_file_not_exists_in_current_dir

logger = logging.getLogger(__name__)


class TxOperation(abc.ABC):
    @abc.abstractmethod
    def execute(self, db: Database, tx: TxId):
        pass


@dataclass
class TxInsertOp(TxOperation):
    record: DbRecord

    def execute(self, db: Database, tx_id: TxId):
        db.txt_insert(tx_id, self.record)


@dataclass
class TxUpdateOp(TxOperation):
    record: DbRecord

    def execute(self, db: Database, tx_id: TxId):
        db.txt_update(tx_id, self.record)


@dataclass
class TxDeleteOp(TxOperation):
    record: DbRecord

    def execute(self, db: Database, tx_id: TxId):
        db.txt_delete(tx_id, self.record.key)


@dataclass
class TxReadOp(TxOperation):
    record: DbRecord

    def execute(self, db: Database, tx_id: TxId):
        db.txt_read(tx_id, self.record.key)


@dataclass
class TxIncrementIntOp(TxOperation):
    record: DbRecord

    def execute(self, db: Database, tx_id: TxId):
        current_val = int(db.txt_read(tx_id, self.record.key).record.value)
        current_val += 1
        new_record = DbRecord(self.record.key, current_val)
        db.txt_update(tx_id, new_record)


@dataclass
class TxConcatStrOp(TxOperation):
    record: DbRecord
    add_string: str

    def execute(self, db: Database, tx_id: TxId):
        current_val = db.txt_read(tx_id, self.record.key).record.value
        current_val += self.add_string
        new_record = DbRecord(self.record.key, current_val)
        db.txt_update(tx_id, new_record)


class Transaction:
    def __init__(self):
        self._ops: typing.List[TxOperation] = []

    def add(self, operation: TxOperation):
        self._ops.append(operation)

    def execute(self, db: Database, abort=False):
        tx_id = db.begin_transaction()
        for op in self._ops:
            op.execute(db, tx_id)
        if abort:
            db.abort(tx_id)
        else:
            db.commit(tx_id)


def threaded_insert(chunk: List[DbRecord], db: Database) -> None:
    for rec in chunk:
        transaction = Transaction()
        transaction.add(TxInsertOp(rec))
        transaction.execute(db)


class TestDbEngine(TestCase):
    def setUp(self) -> None:
        self.test_db_file_path = ensure_file_not_exists_in_current_dir('db.txt')

    def test_should_properly_store_data(self):
        # given
        db = Database(self.test_db_file_path)

        # when
        db.insert(DbRecord(DbKey("myId1"), "Hello"))
        db.insert(DbRecord(DbKey("myId2"), "World"))

        # expect
        self.assertEqual(db.read(DbKey("myId1")).record.value, "Hello")
        self.assertEqual(db.read(DbKey("myId2")).record.value, "World")

    def test_should_properly_store_random_data(self):
        # given
        db = Database(self.test_db_file_path)

        # when
        indexes = []
        for i in range(200):
            s = random_string(random.randint(0, 100))
            indexes.append(s)
            db.insert(DbRecord(DbKey(f"id{s}"), f"value{s}"))

        # expect
        for idx in indexes:
            self.assertEqual(f"value{idx}", db.read(DbKey(f"id{idx}")).record.value)

    def test_should_handle_concurrent_transactions(self):
        # given
        db = Database(self.test_db_file_path)
        existing_records = [i for i in range(10000)]
        records_to_insert = [DbRecord(DbKey(f"id{i}"), f"val{i}") for i in
                             range(len(existing_records), int(1.25 * len(existing_records)))]
        insert_threads = 1
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
            self.assertEqual(db.read(rec.key).record.value, rec.value)


def divide_into_chunks(records: List[typing.Any], chunks: int) -> List[List[typing.Any]]:
    chunk_size = len(records) // chunks
    result = [records[i * chunk_size: (i + 1) * chunk_size] for i in range(chunks - 1)]
    result.append(records[(chunks - 1) * chunk_size:])
    return result
