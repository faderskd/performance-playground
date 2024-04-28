import random
from unittest import TestCase

from apps.broker.transactions.database import Database
from apps.broker.transactions.record import DbRecord, DbKey, DbRecordDoesNotExists
from tests.test_transactions import ThreadingUtilsMixin
from tests.test_utils import ensure_file_not_exists_in_current_dir, random_string


class TestDbEngine(ThreadingUtilsMixin, TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.test_db_file_path = ensure_file_not_exists_in_current_dir('db.txt')

    def test_should_properly_store_data(self):
        # given
        db = Database(self.test_db_file_path)

        # when
        db.insert(DbRecord(DbKey("myId1"), "Hello"))
        db.insert(DbRecord(DbKey("myId2"), "World"))

        # expect
        self.assertEqual(db.read(DbKey("myId1")).value, "Hello")
        self.assertEqual(db.read(DbKey("myId2")).value, "World")

    def test_should_properly_store_random_data(self):
        # given
        db = Database(self.test_db_file_path)

        # when
        indexes = []
        random_data = {random_string(random.randint(0, 1000)) for _ in range(200)}
        for s in random_data:
            indexes.append(s)
            db.insert(DbRecord(DbKey(f"id{s}"), f"value{s}"))

        # expect
        for idx in indexes:
            self.assertEqual(f"value{idx}", db.read(DbKey(f"id{idx}")).value)

    def test_should_properly_delete_data(self):
        # given
        db = Database(self.test_db_file_path)
        db.insert(DbRecord(DbKey("myId1"), "Hello"))
        self.assertEqual(db.read(DbKey("myId1")).value, "Hello")

        # when
        db.delete(DbKey("myId1"))

        # then
        with self.assertRaises(DbRecordDoesNotExists):
            db.read(DbKey("myId1"))
