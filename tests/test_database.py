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


class TestBenchmark(TestCase):
    def setUp(self) -> None:
        # TODO remove db file or create a temporary one
        self.db = BrokerDb()
        self.records_count = 100_000
        self.should_stop = threading.Event()
        self.records = self._write_init_data_to_db(self.records_count)

    def test_should_run_benchmark(self):
        # given
        read_workers = 4
        reader_thread_pool = ThreadPoolExecutor(max_workers=read_workers)

        read_records_per_thread = self.records_count // read_workers

        records_as_set = {r.data for r in self.records}
        offsets = list(range(self.records_count))

        def reader_job(worker_number):
            def job():
                i = 0
                count = 0
                while not self.should_stop.is_set():
                    start = worker_number * read_records_per_thread
                    curr_index = start + i % read_records_per_thread
                    record = self.db.read_record(offsets[curr_index])
                    assert record.data in records_as_set

                    i += 1
                    count += 1
                return count

            return job

        read_futures = []
        for i in range(read_workers):
            read_futures.append(reader_thread_pool.submit(reader_job(i)))

        threading.Thread(target=self.writer_job).run()

        # then
        total_operations = 0
        for f in read_futures:
            total_operations += f.result()
        logger.info("Total read throughput: %d", total_operations)

    def test_should_run_benchmark_with_profiler(self):
        with profile():
            self.test_should_run_benchmark()

    def _write_init_data_to_db(self, records_count: int) -> List[DbRecord]:
        records = [DbRecord(id=self._random_string(5), data=self._random_string(10)) for _ in range(records_count)]
        for rec_idx in range(self.records_count):
            self.db.append_record(records[rec_idx])

        return records

    def writer_job(self):
        for rec in self.records:
            self.db.append_record(rec)
        self.should_stop.set()

    @staticmethod
    def _random_string(length=10):
        return ''.join([random.choice(string.ascii_uppercase + string.digits) for _ in range(length)])
