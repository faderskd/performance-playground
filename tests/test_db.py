import logging
import os
import random
from unittest import TestCase

from apps.broker.db import BrokerDb, DbRecord
from tests.test_utils import random_string

logger = logging.getLogger(__name__)


class TestDb(TestCase):
    def setUp(self) -> None:
        dir_path = os.path.dirname(os.path.realpath(__file__))
        test_db_file_path = os.path.join(dir_path, 'db')
        if os.path.exists(test_db_file_path):
            os.remove(test_db_file_path)
        self.db = BrokerDb(test_db_file_path)

    def test_should_properly_store_data(self):
        # when
        index1 = self.db.append_record(DbRecord("myId1", "Hello"))
        index2 = self.db.append_record(DbRecord("myId2", "World"))

        # expect
        self.assertEqual(self.db.read_record(index1).data, "Hello")
        self.assertEqual(self.db.read_record(index2).data, "World")

    def test_should_properly_store_random_data(self):
        # when
        indexes = []
        for i in range(200):
            s = random_string(random.randint(0, 100))
            indexes.append((s, self.db.append_record(DbRecord("id", s))))

        # expect
        for idx in indexes:
            self.assertEqual(idx[0], self.db.read_record(idx[1]).data)
