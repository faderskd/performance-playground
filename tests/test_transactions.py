import logging
import random
from unittest import TestCase

from apps.broker.transactions.database import Database, DbKey, DbRecord
from tests.test_utils import random_string, ensure_file_not_exists_in_current_dir

logger = logging.getLogger(__name__)


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
