import os
import random
import unittest

from apps.broker.persistent_btree import PersBTree, PersBTreeNode, PersBTreeNodeLeaf, NodePointer, PersKey
from apps.broker.storage_engine import DbSlotPointer
from tests.test_utils import ensure_file_not_exists_in_current_dir


class TestBTree(unittest.TestCase):
    def setUp(self):
        self.file_path = ensure_file_not_exists_in_current_dir('tree')

    def tearDown(self):
        if os.path.exists(self.file_path):
            os.remove(self.file_path)

    def test_should_properly_serialize_leaf_node(self):
        # given
        leaf = PersBTreeNodeLeaf([PersKey(1), PersKey(2)], [], [DbSlotPointer(1, 1), DbSlotPointer(2, 2)], 3,
                                 NodePointer(1), NodePointer(2), None)

        # when
        binary = leaf.to_binary()

        # then
        deserialized = PersBTreeNodeLeaf.from_binary(binary, 3, None)
        self.assertEqual(deserialized.keys, [PersKey(1), PersKey(2)])
        self.assertEqual(deserialized.values, [DbSlotPointer(1, 1), DbSlotPointer(2, 2)])
        self.assertEqual(deserialized.children, [])
        self.assertEqual(deserialized.next, NodePointer(1))
        self.assertEqual(deserialized.prev, NodePointer(2))

    def test_should_properly_serialize_index_node(self):
        # given
        node = PersBTreeNode([PersKey(1), PersKey(2)], [NodePointer(1), NodePointer(2)], [], 3, None)

        # when
        binary = node.to_binary()

        # then
        deserialized = PersBTreeNodeLeaf.from_binary(binary, 3, None)
        self.assertEqual(deserialized.keys, [PersKey(1), PersKey(2)])
        self.assertEqual(deserialized.values, [])
        self.assertEqual(deserialized.children, [NodePointer(1), NodePointer(2)])

    def test_should_build_proper_tree(self):
        # given
        tree = PersBTree(self.file_path, 3)
        value = DbSlotPointer(0, 0)

        # when
        tree.insert(10, value)
        tree.insert(29, value)
        tree.insert(40, value)
        tree.insert(25, value)
        tree.insert(0, value)
        tree.insert(5, value)
        tree.insert(60, value)
        tree.insert(2, value)
        tree.insert(15, value)
        tree.insert(16, value)
        tree.insert(1, value)
        tree.insert(11, value)

        # then
        self.assertEqual(tree.dfs(), [16, 2, 10, 0, 1, 2, 5, 10, 11, 15, 29, 16, 25, 29, 40, 60])

        # when
        tree.delete(5)
        tree.delete(2)
        tree.delete(1)
        tree.delete(16)
        tree.delete(29)
        tree.delete(25)

        # then
        self.assertEqual(tree.dfs(), [40, 10, 11, 0, 10, 11, 15, 60, 40, 60])

    def test_should_properly_delete_from_tree_leafs_and_merge_with_siblings(self):
        # given
        tree = PersBTree(self.file_path, 3)
        value = DbSlotPointer(0, 0)

        # when
        tree.insert(10, value)
        tree.insert(29, value)
        tree.insert(40, value)
        tree.insert(25, value)
        tree.insert(0, value)
        tree.insert(5, value)
        tree.insert(60, value)
        tree.insert(2, value)
        tree.insert(15, value)
        tree.insert(16, value)
        tree.insert(1, value)

        # then
        self.assertEqual(tree.dfs(), [16, 2, 10, 0, 1, 2, 5, 10, 15, 29, 16, 25, 29, 40, 60])

        # when
        tree.delete(40)
        tree.delete(60)
        tree.delete(29)

        # then
        self.assertEqual(tree.dfs(), [16, 2, 10, 0, 1, 2, 5, 10, 15, 25, 16, 25])

    def test_should_properly_delete_from_tree_indexes_and_borrow_from_left_siblings(self):
        # given
        tree = PersBTree(self.file_path, 3)
        value = DbSlotPointer(0, 0)

        # when
        tree.insert(10, value)
        tree.insert(29, value)
        tree.insert(40, value)
        tree.insert(25, value)
        tree.insert(0, value)
        tree.insert(5, value)
        tree.insert(60, value)
        tree.insert(2, value)
        tree.insert(15, value)
        tree.insert(16, value)
        tree.insert(1, value)
        tree.insert(30, value)

        # then
        self.assertEqual(tree.dfs(), [16, 2, 10, 0, 1, 2, 5, 10, 15, 29, 40, 16, 25, 29, 30, 40, 60])

        # when
        tree.delete(10)
        tree.delete(15)
        tree.delete(0)
        tree.delete(1)
        tree.delete(60)
        tree.delete(40)
        tree.delete(5)

        # then
        self.assertEqual(tree.dfs(), [29, 16, 2, 16, 25, 30, 29, 30])

    def test_should_properly_delete_from_tree_from_both_sides_including_leafs_and_indexes(self):
        # given
        tree = PersBTree(self.file_path, 3)
        value = DbSlotPointer(0, 0)

        # when
        tree.insert(1, value)
        tree.insert(100, value)
        tree.insert(50, value)
        tree.insert(75, value)
        tree.insert(25, value)
        tree.insert(90, value)
        tree.insert(12, value)
        tree.insert(40, value)
        tree.insert(80, value)
        tree.insert(95, value)
        tree.insert(6, value)
        tree.insert(77, value)
        tree.insert(200, value)
        tree.insert(78, value)
        tree.insert(89, value)
        tree.insert(41, value)
        tree.insert(42, value)
        tree.insert(43, value)
        tree.insert(44, value)
        tree.insert(81, value)
        tree.insert(82, value)
        tree.insert(83, value)
        tree.insert(76, value)
        tree.insert(74, value)
        tree.insert(111, value)
        tree.insert(112, value)

        # when
        tree.delete(50)
        tree.delete(74)
        tree.delete(44)
        tree.delete(75)
        tree.delete(41)
        tree.delete(43)

        # then
        self.assertEqual(tree.dfs(),
                         [42, 78, 90, 25, 1, 6, 12, 25, 40, 76, 42, 76, 77, 81, 83, 78, 80, 81, 82, 83, 89, 100, 112,
                          90, 95, 100, 111, 112, 200])

        # when
        tree.insert(50, value)
        tree.insert(48, value)
        tree.insert(49, value)

        tree.delete(25)
        tree.delete(40)
        tree.delete(6)
        tree.delete(1)
        tree.delete(111)
        tree.delete(90)
        tree.delete(78)
        tree.delete(112)
        tree.delete(95)

        # then
        self.assertEqual(tree.dfs(),
                         [49, 80, 100, 42, 12, 42, 48, 76, 49, 50, 76, 77, 81, 83, 80, 81, 82, 83, 89, 200, 100, 200])

        # when
        tree.delete(100)
        tree.insert(78, value)
        tree.insert(79, value)

        tree.delete(80)
        tree.delete(82)
        tree.delete(76)
        tree.delete(79)

        # then
        self.assertEqual(tree.dfs(), [49, 78, 83, 42, 12, 42, 48, 77, 49, 50, 77, 81, 78, 81, 200, 83, 89, 200])

        # when
        tree.insert(60, value)
        tree.insert(61, value)
        tree.insert(62, value)
        tree.insert(63, value)
        tree.insert(64, value)
        tree.insert(65, value)

        tree.delete(49)
        tree.delete(64)

        # then
        self.assertEqual(tree.dfs(),
                         [78, 50, 65, 42, 12, 42, 48, 60, 62, 50, 60, 61, 62, 63, 77, 65, 77, 83, 81, 78, 81, 200, 83,
                          89, 200])

        # when
        tree.delete(65)
        tree.delete(50)

        # then
        self.assertEqual(tree.dfs(),
                         [78, 60, 62, 42, 12, 42, 48, 61, 60, 61, 77, 62, 63, 77, 83, 81, 78, 81, 200, 83, 89, 200])

        # when
        tree.delete(83)
        tree.delete(89)

        # then
        self.assertEqual(tree.dfs(),
                         [62, 60, 42, 12, 42, 48, 61, 60, 61, 78, 77, 62, 63, 77, 81, 200, 78, 81, 200])

        # when
        tree.delete(60)
        tree.delete(77)
        tree.delete(63)

        # then
        self.assertEqual(tree.dfs(), [61, 78, 42, 12, 42, 48, 62, 61, 62, 81, 200, 78, 81, 200])

        # when
        tree.delete(61)
        tree.delete(78)
        tree.delete(200)
        tree.delete(12)
        tree.delete(81)

        # then
        self.assertEqual(tree.dfs(), [48, 62, 42, 48, 62])

        # when
        tree.delete(48)
        tree.delete(62)

        # then
        self.assertEqual(tree.dfs(), [42])

        # when
        tree.delete(42)

        # then
        self.assertEqual(tree.dfs(), [])

    def test_should_delete_from_tree_with_when_there_are_not_empty_keys_index_after_deletion(self):
        # given
        tree = PersBTree(self.file_path, 4)
        value = DbSlotPointer(0, 0)

        for i in range(1, 73):
            tree.insert(i, value)

        # when
        tree.delete(23)

        # then
        self.assertEqual(tree.dfs(),
                         [19, 43, 7, 13, 3, 5, 1, 2, 3, 4, 5, 6, 9, 11, 7, 8, 9, 10, 11, 12, 15, 17, 13, 14, 15, 16, 17,
                          18, 31, 37, 21, 25, 27, 29, 19, 20, 21, 22, 24, 25, 26, 27, 28, 29, 30, 33, 35, 31, 32, 33,
                          34, 35, 36, 39, 41, 37, 38, 39, 40, 41, 42, 49, 55, 61, 45, 47, 43, 44, 45, 46, 47, 48, 51,
                          53, 49, 50, 51, 52, 53, 54, 57, 59, 55, 56, 57, 58, 59, 60, 63, 65, 67, 69, 61, 62, 63, 64,
                          65, 66, 67, 68, 69, 70, 71, 72])

        # when
        tree.delete(31)

        # then
        self.assertEqual(tree.dfs(),
                         [19, 43, 7, 13, 3, 5, 1, 2, 3, 4, 5, 6, 9, 11, 7, 8, 9, 10, 11, 12, 15, 17, 13, 14, 15, 16, 17,
                          18, 29, 37, 21, 25, 27, 19, 20, 21, 22, 24, 25, 26, 27, 28, 32, 35, 29, 30, 32, 33, 34, 35,
                          36, 39, 41, 37, 38, 39, 40, 41, 42, 49, 55, 61, 45, 47, 43, 44, 45, 46, 47, 48, 51, 53, 49,
                          50, 51, 52, 53, 54, 57, 59, 55, 56, 57, 58, 59, 60, 63, 65, 67, 69, 61, 62, 63, 64, 65, 66,
                          67, 68, 69, 70, 71, 72])

        # when
        tree.delete(43)

        # then
        self.assertEqual(tree.dfs(),
                         [19, 44, 7, 13, 3, 5, 1, 2, 3, 4, 5, 6, 9, 11, 7, 8, 9, 10, 11, 12, 15, 17, 13, 14, 15, 16, 17,
                          18, 29, 37, 21, 25, 27, 19, 20, 21, 22, 24, 25, 26, 27, 28, 32, 35, 29, 30, 32, 33, 34, 35,
                          36, 39, 41, 37, 38, 39, 40, 41, 42, 55, 61, 47, 49, 51, 53, 44, 45, 46, 47, 48, 49, 50, 51,
                          52, 53, 54, 57, 59, 55, 56, 57, 58, 59, 60, 63, 65, 67, 69, 61, 62, 63, 64, 65, 66, 67, 68,
                          69, 70, 71, 72])

        # when
        tree.delete(49)
        tree.delete(55)
        tree.delete(69)
        tree.delete(70)
        tree.delete(71)
        tree.delete(72)
        tree.delete(44)
        tree.delete(45)
        tree.delete(46)

        # then
        self.assertEqual(tree.dfs(),
                         [19, 7, 13, 3, 5, 1, 2, 3, 4, 5, 6, 9, 11, 7, 8, 9, 10, 11, 12, 15, 17, 13, 14, 15, 16, 17, 18,
                          29, 37, 47, 61, 21, 25, 27, 19, 20, 21, 22, 24, 25, 26, 27, 28, 32, 35, 29, 30, 32, 33, 34,
                          35, 36, 39, 41, 37, 38, 39, 40, 41, 42, 51, 53, 56, 59, 47, 48, 50, 51, 52, 53, 54, 56, 57,
                          58, 59, 60, 63, 65, 67, 61, 62, 63, 64, 65, 66, 67, 68])

        # when
        tree.delete(13)

        # then
        self.assertEqual(tree.dfs(),
                         [29, 7, 19, 3, 5, 1, 2, 3, 4, 5, 6, 9, 11, 14, 17, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17, 18, 21,
                          25, 27, 19, 20, 21, 22, 24, 25, 26, 27, 28, 37, 47, 61, 32, 35, 29, 30, 32, 33, 34, 35, 36,
                          39, 41, 37, 38, 39, 40, 41, 42, 51, 53, 56, 59, 47, 48, 50, 51, 52, 53, 54, 56, 57, 58, 59,
                          60, 63, 65, 67, 61, 62, 63, 64, 65, 66, 67, 68])

        # when
        tree.delete(19)
        tree.delete(14)
        tree.delete(17)
        tree.delete(10)
        tree.delete(25)
        tree.delete(37)
        tree.delete(56)
        tree.delete(53)
        tree.delete(39)
        tree.delete(47)
        tree.delete(51)
        tree.delete(52)
        tree.delete(54)
        tree.delete(57)
        tree.delete(58)
        tree.delete(48)
        tree.delete(59)
        tree.delete(22)
        tree.delete(33)

        # then
        self.assertEqual(tree.dfs(),
                         [29, 7, 20, 3, 5, 1, 2, 3, 4, 5, 6, 11, 15, 7, 8, 9, 11, 12, 15, 16, 18, 24, 27, 20, 21, 24,
                          26, 27, 28, 38, 63, 32, 35, 29, 30, 32, 34, 35, 36, 41, 50, 61, 38, 40, 41, 42, 50, 60, 61,
                          62, 65, 67, 63, 64, 65, 66, 67, 68])

        # when
        tree.delete(29)

        # then
        self.assertEqual(tree.dfs(),
                         [30, 7, 20, 3, 5, 1, 2, 3, 4, 5, 6, 11, 15, 7, 8, 9, 11, 12, 15, 16, 18, 24, 27, 20, 21, 24,
                          26, 27, 28, 41, 63, 35, 38, 30, 32, 34, 35, 36, 38, 40, 50, 61, 41, 42, 50, 60, 61, 62, 65,
                          67, 63, 64, 65, 66, 67, 68])

        # when
        tree.delete(11)
        tree.delete(15)
        tree.delete(30)
        tree.delete(24)

        # then
        self.assertEqual(tree.dfs(),
                         [7, 32, 41, 63, 3, 5, 1, 2, 3, 4, 5, 6, 9, 16, 20, 27, 7, 8, 9, 12, 16, 18, 20, 21, 26, 27, 28,
                          35, 38, 32, 34, 35, 36, 38, 40, 50, 61, 41, 42, 50, 60, 61, 62, 65, 67, 63, 64, 65, 66, 67,
                          68]
                         )

        # when
        tree.delete(35)
        tree.delete(34)
        tree.delete(36)
        tree.delete(20)
        tree.delete(27)
        tree.delete(21)
        tree.delete(32)
        tree.delete(26)
        tree.delete(28)
        tree.delete(16)
        tree.delete(40)
        tree.delete(63)
        tree.delete(62)
        tree.delete(41)
        tree.delete(42)
        tree.delete(7)
        tree.delete(8)
        tree.delete(9)
        tree.delete(2)
        tree.delete(1)

        # then
        self.assertEqual(tree.dfs(),
                         [61, 5, 12, 50, 3, 4, 5, 6, 12, 18, 38, 50, 60, 65, 67, 61, 64, 65, 66, 67, 68]
                         )

        # when
        tree.delete(3)
        tree.delete(61)
        tree.delete(4)
        tree.delete(5)
        tree.delete(6)
        tree.delete(12)
        tree.delete(18)
        tree.delete(38)
        tree.delete(50)
        tree.delete(60)
        tree.delete(64)
        tree.delete(65)
        tree.delete(68)
        tree.delete(66)
        tree.delete(67)

        # then
        self.assertEqual(tree.dfs(), [])

    def test_should_properly_remove_random_keys_tree_max_keys_odd(self):
        # given
        tree = PersBTree(self.file_path, 3)

        large_array = [i for i in range(1000)]
        large_array_as_set = set(large_array)
        random.shuffle(large_array)

        for k in large_array:
            tree.insert(k, DbSlotPointer(0, 0))

        for i, k in enumerate(large_array):
            # when
            tree.delete(k)

            # then
            large_array_as_set.remove(k)
            self.assertEqual([t.key for t in tree.get_leafs()], sorted(large_array_as_set))

    def test_should_properly_remove_random_keys_tree_max_keys_even(self):
        # given
        tree = PersBTree(self.file_path, 6)

        large_array = [i for i in range(1000)]
        large_array_as_set = set(large_array)
        random.shuffle(large_array)

        for k in large_array:
            tree.insert(k, DbSlotPointer(0, 0))

        for i, k in enumerate(large_array):
            # when
            tree.delete(k)

            # then
            large_array_as_set.remove(k)
            self.assertEqual([t.key for t in tree.get_leafs()], sorted(large_array_as_set))

    def test_should_preserve_not_deleted_values(self):
        # given
        tree = PersBTree(self.file_path, 5)

        large_array = [i for i in range(10000)]
        random.shuffle(large_array)

        for k in large_array:
            tree.insert(k, DbSlotPointer(k, k))

        # when
        for i, k in enumerate(large_array):
            if len(large_array) - i <= 100:
                break
            tree.delete(k)

        # then
        self.assertEqual(len(tree.get_leafs()), 100)

        # when
        for k in large_array[len(large_array) - 100:]:
            self.assertEqual(tree.find(k), DbSlotPointer(k, k))
