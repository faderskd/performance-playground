import logging
import string
import threading
from concurrent.futures.thread import ThreadPoolExecutor
import random
from typing import List
from unittest import TestCase

from apps.broker.db import BrokerDb, DbRecord
from tests.profiler_utils import profile

logger = logging.getLogger(__name__)


class TestDb(TestCase):
    def setUp(self) -> None:
        # TODO remove db file or create a temporary one
        self.db = BrokerDb()

    def test_should_properly_read_data(self):
        # when
        offset1 = self.db.append_record(DbRecord("myId1", "Hello"))
        offset2 = self.db.append_record(DbRecord("myId2", "World"))

        # expect
        self.assertEqual(self.db.read_record(offset1).data, "Hello")
        self.assertEqual(self.db.read_record(offset2).data, "World")
