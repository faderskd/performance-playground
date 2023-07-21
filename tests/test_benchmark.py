import string
from concurrent.futures.thread import ThreadPoolExecutor
from unittest import TestCase
import random

from apps.broker.db import BrokerDb, DbRecord


class TestBenchmark(TestCase):
    def setUp(self) -> None:
        # TODO remove db file or create a temporary one
        pass


    def test_should_run_benchmark(self):
        # given
        db = BrokerDb()

        write_workers = 4
        writer_thread_pool = ThreadPoolExecutor(max_workers=write_workers)

        read_workers = 4
        reader_thread_pool = ThreadPoolExecutor(max_workers=read_workers)

        records_count = 100_:000
        write_records_per_thread = records_count // write_workers
        read_records_per_thread = records_count // read_workers

        records = [DbRecord(id=self.random_string(5), data=self.random_string(10)) for _ in range(records_count)]
        records_as_set = {r.data for r in records}
        offsets = list(range(0, records_count))
        random.shuffle(offsets)

        def writer_job(worker_number):
            def job():
                start = worker_number * write_records_per_thread
                for rec_idx in range(start, start + write_records_per_thread):
                    db.append_record(records[rec_idx])

            return job

        def reader_job(worker_number):
            def job():
                start = worker_number * read_records_per_thread
                for rec_idx in range(start, start + read_records_per_thread):
                    record = db.read_record(offsets[rec_idx])
                    self.assertTrue(record.data in records_as_set)

            return job

        # when
        write_futures = []
        for i in range(write_workers):
            write_futures.append(writer_thread_pool.submit(writer_job(i)))

        for f in write_futures:
            f.result()

        read_futures = []
        for i in range(read_workers):
            read_futures.append(reader_thread_pool.submit(reader_job(i)))

        # then
        for f in read_futures:
            f.result()

    def random_string(self, length=10):
        return ''.join([random.choice(string.ascii_uppercase + string.digits) for _ in range(length)])
