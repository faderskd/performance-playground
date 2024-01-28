import os
import random
import unittest
from concurrent.futures import ThreadPoolExecutor

from apps.broker.index.persistent_btree import PersBTree
from apps.broker.storage_engine import DbRecordPointer
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
        arr_len = 24
        large_array = [i for i in range(arr_len)]
        count_per_thread = arr_len // thread_count
        # random.shuffle(large_array)

        max_pointer_block = max_pointer_slot = 2^16 # two bytes
        def threaded_insert(chunk):
            for k in chunk:
                tree.insert(k, DbRecordPointer(k % max_pointer_block, k % max_pointer_slot))

        chunks = [large_array[i * count_per_thread:i * count_per_thread + count_per_thread] for i in
                  range(thread_count)]
        futures = [executor.submit(threaded_insert, c) for c in chunks]
        for f in futures:
            f.result()

        # when
        # for i, k in enumerate(large_array):
        #     if len(large_array) - i <= 100:
        #         break
        #     tree.delete(k)

        # then
        # self.assertEqual(sorted(large_array), [t.key for t in tree.get_leafs()])

        # and
        for k in large_array:
            self.assertEqual(tree.find(k), DbRecordPointer(k % max_pointer_block, k % max_pointer_slot))
