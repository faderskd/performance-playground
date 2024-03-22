import logging
import threading
import typing
from concurrent.futures.thread import ThreadPoolExecutor
from typing import List
from unittest import TestCase

from apps.broker.storage.storage_engine import DbEngine, DbRecord
from tests.profiler_utils import profile
from tests.test_utils import random_string, ensure_file_not_exists_in_current_dir

logger = logging.getLogger(__name__)


class TestBenchmark(TestCase):
    def setUp(self) -> None:
        test_db_file_path = ensure_file_not_exists_in_current_dir('db')
        self.db = DbEngine(test_db_file_path)

    def test_should_concurrently_append_records(self):
        # given
        workers = 10
        records_count = 1000
        reader_thread_pool = ThreadPoolExecutor(max_workers=workers)
        records_to_insert = [i for i in range(records_count)]

        chunks = self.divide_into_chunks(records_to_insert, workers)

        def writer_job(chunk: List[int]):
            record_pointers = []
            for k in chunk:
                pointer = self.db.append_record(DbRecord(str(k), str(k)))
                record_pointers.append(pointer)
            return record_pointers

        write_futures = []
        for c in chunks:
            write_futures.append(reader_thread_pool.submit(writer_job, c))

        # then
        pointers = []
        for f in write_futures:
            pointers += f.result()

        result_values = []
        for pointer in pointers:
            result_values.append(int(self.db.read_record(pointer).data))

        self.assertEqual(sorted(result_values), records_to_insert)

    @staticmethod
    def divide_into_chunks(records: List[typing.Any], chunks: int) -> List[List[typing.Any]]:
        chunk_size = len(records) // chunks
        result = [records[i * chunk_size: (i + 1) * chunk_size] for i in range(chunks - 1)]
        result.append(records[(chunks - 1) * chunk_size:])
        return result

    # def test_should_run_benchmark_with_profiler(self):
    #     with profile():
    #         self.test_should_run_benchmark()
