import os
import random
import typing
import unittest
from concurrent.futures import ThreadPoolExecutor

from apps.broker.index.persistent_btree import PersBTree
from apps.broker.storage.storage_engine import DbRecordPointer
from tests.test_utils import ensure_file_not_exists_in_current_dir


class TestBTree(unittest.TestCase):
    def setUp(self):
        self.file_path = ensure_file_not_exists_in_current_dir('tree')

    def tearDown(self):
        if os.path.exists(self.file_path):
            os.remove(self.file_path)

    def test_should_insert_concurrently(self):
        with PersBTree(self.file_path, 5) as tree:
            # given
            thread_count = 10
            executor = ThreadPoolExecutor(max_workers=thread_count, thread_name_prefix="test-concurrent-insert")
            arr_len = 10000
            large_array = [i for i in range(arr_len)]
            random.shuffle(large_array)

            max_pointer_block = max_pointer_slot = 2 ^ 16  # two bytes

            def threaded_insert(chunk):
                for k in chunk:
                    tree.insert(k, DbRecordPointer(k % max_pointer_block, k % max_pointer_slot))

            chunks = self._divide_into_chunks(large_array, thread_count)
            futures = [executor.submit(threaded_insert, c) for c in chunks]
            for f in futures:
                f.result()

            # then
            self.assertEqual(sorted(large_array), [t.key for t in tree.get_leafs()])

            # and
            for k in large_array:
                self.assertEqual(tree.find(k), DbRecordPointer(k % max_pointer_block, k % max_pointer_slot))

    def test_should_delete_concurrently(self):
        with PersBTree(self.file_path, 5) as tree:
            # given
            thread_count = 10
            executor = ThreadPoolExecutor(max_workers=thread_count, thread_name_prefix="test-concurrent-delete")
            arr_len = 10000
            removed_count = arr_len // 2
            large_array = [i for i in range(arr_len)]
            random.shuffle(large_array)

            elements_left = set(large_array)
            elements_to_delete = large_array[:removed_count]
            for e in elements_to_delete:
                elements_left.remove(e)

            chunks = self._divide_into_chunks(elements_to_delete, thread_count)

            max_pointer_block = max_pointer_slot = 2 ^ 16  # two bytes
            for k in large_array:
                tree.insert(k, DbRecordPointer(k % max_pointer_block, k % max_pointer_slot))

            def threaded_delete(chunk):
                for k in chunk:
                    tree.delete(k)

            futures = [executor.submit(threaded_delete, c) for c in chunks]
            for f in futures:
                f.result()

            # then
            self.assertEqual(sorted(elements_left), [t.key for t in tree.get_leafs()])

            # and
            for k in elements_left:
                self.assertEqual(tree.find(k), DbRecordPointer(k % max_pointer_block, k % max_pointer_slot))

            # and
            for k in elements_to_delete:
                self.assertEqual(tree.find(k), None)

    def test_should_insert_find_and_delete_concurrently(self):
        with PersBTree(self.file_path, 5) as tree:
            # given
            thread_count = 40
            executor = ThreadPoolExecutor(max_workers=thread_count, thread_name_prefix="test-concurrent-insert-delete")
            arr_len = 16000
            part_length = arr_len // 4
            large_array = [i for i in range(arr_len)]
            random.shuffle(large_array)

            elements_left = large_array[:part_length]
            elements_to_delete = large_array[part_length:2 * part_length]
            elements_not_touched = large_array[2 * part_length:3 * part_length]
            elements_updated = large_array[3 * part_length:]

            max_pointer_block = max_pointer_slot = 2 ^ 16  # two bytes

            for e in elements_to_delete + elements_not_touched + elements_updated:
                tree.insert(e, DbRecordPointer(e % max_pointer_block, e % max_pointer_slot))

            insert_chunks = self._divide_into_chunks(elements_left, thread_count // 4)
            delete_chunks = self._divide_into_chunks(elements_to_delete, thread_count // 4)
            find_chunks = self._divide_into_chunks(elements_not_touched, thread_count // 4)
            update_chunks = self._divide_into_chunks(elements_updated, thread_count // 4)

            def threaded_insert(chunk):
                for k in chunk:
                    tree.insert(k, DbRecordPointer(k % max_pointer_block, k % max_pointer_slot))

            def threaded_delete(chunk):
                for k in chunk:
                    tree.delete(k)

            def threaded_find(chunk):
                for k in chunk:
                    assert tree.find(k) == DbRecordPointer(k % max_pointer_block, k % max_pointer_slot)

            def threaded_update(chunk):
                for k in chunk:
                    tree.update(k, DbRecordPointer((k + 1) % max_pointer_block, (k + 1) % max_pointer_slot))

            insert_futures = [executor.submit(threaded_insert, c) for c in insert_chunks]
            delete_futures = [executor.submit(threaded_delete, c) for c in delete_chunks]
            find_futures = [executor.submit(threaded_find, c) for c in find_chunks]
            update_futures = [executor.submit(threaded_update, c) for c in update_chunks]

            for f in insert_futures + delete_futures + find_futures + update_futures:
                f.result()

            # then
            self.assertEqual(sorted(elements_left + elements_not_touched + elements_updated), [t.key for t in tree.get_leafs()])

            # and
            for k in elements_left:
                self.assertEqual(tree.find(k), DbRecordPointer(k % max_pointer_block, k % max_pointer_slot))

            # and
            for k in elements_to_delete:
                self.assertEqual(tree.find(k), None)

            # and
            for k in elements_updated:
                self.assertEqual(tree.find(k), DbRecordPointer((k + 1) % max_pointer_block, (k + 1) % max_pointer_slot))

    @staticmethod
    def _divide_into_chunks(array, chunks_count) -> typing.List[typing.List[int]]:
        return [array[i::chunks_count] for i in range(chunks_count)]
