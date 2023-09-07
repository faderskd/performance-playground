import logging
import os
from unittest import TestCase

from apps.broker.db import BrokerDb, DbRecord

logger = logging.getLogger(__name__)


class TestDb(TestCase):
    def setUp(self) -> None:
        dir_path = os.path.dirname(os.path.realpath(__file__))
        test_db_file = os.path.join(dir_path, 'db')
        if os.path.exists(test_db_file):
            os.remove(test_db_file)
        self.db = BrokerDb()

    def test_should_properly_read_data(self):
        # when
        index1 = self.db.append_record(DbRecord("myId1", "Hello"))
        index2 = self.db.append_record(DbRecord("myId2", "World"))

        # expect
        self.assertEqual(self.db.read_record(index1).data, "Hello")
        self.assertEqual(self.db.read_record(index2).data, "World")
