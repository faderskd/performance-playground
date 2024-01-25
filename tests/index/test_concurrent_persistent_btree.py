import os
import random
import unittest
from concurrent.futures import ThreadPoolExecutor

from apps.broker.index.persistent_btree import PersBTree
from apps.broker.storage_engine import DbSlotPointer
from tests.test_utils import ensure_file_not_exists_in_current_dir


class TestBTree(unittest.TestCase):
    def setUp(self):
        self.file_path = ensure_file_not_exists_in_current_dir('tree')

    def tearDown(self):
        if os.path.exists(self.file_path):
            os.remove(self.file_path)

    def test_should_insert_concurrently(self):
        # given
        tree = PersBTree(self.file_path, 5)
        thread_count = 1
        executor = ThreadPoolExecutor(max_workers=thread_count, thread_name_prefix="test-concurrent-insert")
        arr_len = 100000
        large_array = [i for i in range(arr_len)]
        count_per_thread = arr_len // thread_count
        # random.shuffle(large_array)

        def threaded_insert(chunk):
            for k in chunk:
                tree.insert(k, DbSlotPointer(k, k))

        chunks = [large_array[i * count_per_thread:i * count_per_thread + count_per_thread] for i in
                  range(thread_count)]
        executor.map(threaded_insert, chunks)
        executor.shutdown()
        # when
        # for i, k in enumerate(large_array):
        #     if len(large_array) - i <= 100:
        #         break
        #     tree.delete(k)

        # then
        # self.assertEqual(sorted(large_array), [t.key for t in tree.get_leafs()])

        # and
        for k in large_array:
            self.assertEqual(tree.find(k), DbSlotPointer(k, k))
