import random
import typing
import unittest

from apps.broker.b_tree_index import BTree, BTreeNode


class TestBTree(unittest.TestCase):
    def test_should_build_proper_tree(self):
        # given
        tree = BTree(3)

        # when
        tree.insert(1, "va1")
        tree.insert(20, "val2")
        tree.insert(10, "val3")
        tree.insert(100, "val4")
        tree.insert(5, "val5")
        tree.insert(6, "val6")
        tree.insert(7, "val7")
        tree.insert(2, "val8")
        tree.insert(2, "val9")
        tree.insert(2, "val10")
        tree.insert(2, "val11")
        tree.insert(2, "val12")
        tree.insert(2, "val13")
        tree.insert(2, "val14")
        tree.insert(2, "val15")
        tree.insert(0, "val16")
        tree.insert(0, "val17")
        tree.insert(0, "val18")
        tree.insert(0, "val19")
        tree.insert(0, "val20")

        # then
        self.assertEqual(self._dfs(tree),
                         [2, 2, 6, 0, 1, 0, 0, 0, 0, 0, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 5, 20, 6, 7, 10, 20, 100])

    def test_should_properly_delete_from_tree_leafs_and_borrow_from_siblings(self):
        # given
        tree = BTree(3)

        # when
        tree.insert(10, "val1")
        tree.insert(29, "val2")
        tree.insert(40, "val3")
        tree.insert(25, "val4")
        tree.insert(0, "val5")
        tree.insert(5, "val6")
        tree.insert(60, "val7")
        tree.insert(2, "val8")
        tree.insert(15, "val9")
        tree.insert(16, "val10")
        tree.insert(1, "val11")

        # then
        self.assertEqual(self._dfs(tree), [16, 2, 10, 0, 1, 2, 5, 10, 15, 29, 16, 25, 29, 40, 60])

        # when
        tree.delete(5)
        tree.delete(2)
        tree.delete(1)
        tree.delete(16)
        tree.delete(29)
        tree.delete(25)

        # then
        self.assertEqual(self._dfs(tree), [40, 10, 15, 0, 10, 15, 60, 40, 60])

    def test_should_properly_delete_from_tree_leafs_and_merge_with_siblings(self):
        # given
        tree = BTree(3)

        # when
        tree.insert(10, "val1")
        tree.insert(29, "val2")
        tree.insert(40, "val3")
        tree.insert(25, "val4")
        tree.insert(0, "val5")
        tree.insert(5, "val6")
        tree.insert(60, "val7")
        tree.insert(2, "val8")
        tree.insert(15, "val9")
        tree.insert(16, "val10")
        tree.insert(1, "val11")

        # then
        self.assertEqual(self._dfs(tree), [16, 2, 10, 0, 1, 2, 5, 10, 15, 29, 16, 25, 29, 40, 60])

        # when
        tree.delete(40)
        tree.delete(60)
        tree.delete(29)

        # then
        self.assertEqual(self._dfs(tree), [16, 2, 10, 0, 1, 2, 5, 10, 15, 25, 16, 25])

    def test_should_properly_delete_from_tree_indexes_and_borrow_from_left_siblings(self):
        # given
        tree = BTree(3)

        # when
        tree.insert(10, "val1")
        tree.insert(29, "val2")
        tree.insert(40, "val3")
        tree.insert(25, "val4")
        tree.insert(0, "val5")
        tree.insert(5, "val6")
        tree.insert(60, "val7")
        tree.insert(2, "val8")
        tree.insert(15, "val9")
        tree.insert(16, "val10")
        tree.insert(1, "val11")
        tree.insert(30, "val12")

        # then
        self.assertEqual(self._dfs(tree), [16, 2, 10, 0, 1, 2, 5, 10, 15, 29, 40, 16, 25, 29, 30, 40, 60])

        # when
        tree.delete(10)
        tree.delete(15)
        tree.delete(0)
        tree.delete(1)
        tree.delete(60)
        tree.delete(40)
        tree.delete(5)

        # then
        self.assertEqual(self._dfs(tree), [29, 16, 2, 16, 25, 30, 29, 30])

    def test_should_properly_delete_from_tree_from_both_sides_including_leafs_and_indexes(self):
        # given
        tree = BTree(3)

        # when
        tree.insert(1, "val1")
        tree.insert(100, "val1")
        tree.insert(50, "val1")
        tree.insert(75, "val1")
        tree.insert(25, "val1")
        tree.insert(90, "val1")
        tree.insert(12, "val1")
        tree.insert(40, "val1")
        tree.insert(80, "val1")
        tree.insert(95, "val1")
        tree.insert(6, "val1")
        tree.insert(77, "val1")
        tree.insert(200, "val1")
        tree.insert(78, "val1")
        tree.insert(89, "val1")
        tree.insert(41, "val1")
        tree.insert(42, "val1")
        tree.insert(43, "val1")
        tree.insert(44, "val1")
        tree.insert(81, "val1")
        tree.insert(82, "val1")
        tree.insert(83, "val1")
        tree.insert(76, "val1")
        tree.insert(74, "val1")
        tree.insert(111, "val1")
        tree.insert(112, "val1")

        # when
        tree.delete(50)
        tree.delete(74)
        tree.delete(44)
        tree.delete(75)
        tree.delete(41)
        tree.delete(43)

        # then
        self.assertEqual(self._dfs(tree),
                         [42, 78, 90, 25, 1, 6, 12, 25, 40, 76, 42, 76, 77, 81, 83, 78, 80, 81, 82, 83, 89, 100, 112,
                          90, 95, 100, 111, 112, 200])

        # when
        tree.insert(50, "val1")
        tree.insert(48, "val2")
        tree.insert(49, "val3")

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
        self.assertEqual(self._dfs(tree),
                         [49, 80, 100, 42, 12, 42, 48, 76, 49, 50, 76, 77, 81, 83, 80, 81, 82, 83, 89, 200, 100, 200])

        # when
        tree.delete(100)
        tree.insert(78, "val1")
        tree.insert(79, "val2")

        tree.delete(80)
        tree.delete(82)
        tree.delete(76)
        tree.delete(79)

        # then
        self.assertEqual(self._dfs(tree), [49, 78, 83, 42, 12, 42, 48, 77, 49, 50, 77, 81, 78, 81, 200, 83, 89, 200])

        # when
        tree.insert(60, "val1")
        tree.insert(61, "val1")
        tree.insert(62, "val1")
        tree.insert(63, "val1")
        tree.insert(64, "val1")
        tree.insert(65, "val1")

        tree.delete(49)
        tree.delete(64)

        # then
        self.assertEqual(self._dfs(tree),
                         [78, 50, 65, 42, 12, 42, 48, 60, 62, 50, 60, 61, 62, 63, 77, 65, 77, 83, 81, 78, 81, 200, 83,
                          89, 200])

        # when
        tree.delete(65)
        tree.delete(50)

        # then
        self.assertEqual(self._dfs(tree),
                         [78, 60, 62, 42, 12, 42, 48, 61, 60, 61, 77, 62, 63, 77, 83, 81, 78, 81, 200, 83, 89, 200])

        # when
        tree.delete(83)
        tree.delete(89)

        # then
        self.assertEqual(self._dfs(tree),
                         [62, 60, 42, 12, 42, 48, 61, 60, 61, 78, 77, 62, 63, 77, 81, 200, 78, 81, 200])

        # when
        tree.delete(60)
        tree.delete(77)
        tree.delete(63)

        # then
        self.assertEqual(self._dfs(tree), [61, 78, 42, 12, 42, 48, 62, 61, 62, 81, 200, 78, 81, 200])

        # when
        tree.delete(61)
        tree.delete(78)
        tree.delete(200)
        tree.delete(12)
        tree.delete(81)

        # then
        self.assertEqual(self._dfs(tree), [48, 62, 42, 48, 62])

        # when
        tree.delete(48)
        tree.delete(62)

        # then
        self.assertEqual(self._dfs(tree), [42])

        # when
        tree.delete(42)

        # then
        self.assertEqual(self._dfs(tree), [])

    def test_should_delete_from_tree_with_when_there_are_not_empty_keys_index_after_deletion(self):
        # given
        tree = BTree(4)

        for i in range(1, 73):
            tree.insert(i, "val")

        # when
        tree.delete(23)

        # then
        self.assertEqual(self._dfs(tree),
                         [19, 43, 7, 13, 3, 5, 1, 2, 3, 4, 5, 6, 9, 11, 7, 8, 9, 10, 11, 12, 15, 17, 13, 14, 15, 16, 17,
                          18, 31, 37, 21, 25, 27, 29, 19, 20, 21, 22, 24, 25, 26, 27, 28, 29, 30, 33, 35, 31, 32, 33,
                          34, 35, 36, 39, 41, 37, 38, 39, 40, 41, 42, 49, 55, 61, 45, 47, 43, 44, 45, 46, 47, 48, 51,
                          53, 49, 50, 51, 52, 53, 54, 57, 59, 55, 56, 57, 58, 59, 60, 63, 65, 67, 69, 61, 62, 63, 64,
                          65, 66, 67, 68, 69, 70, 71, 72])

        # when
        tree.delete(31)

        # then
        self.assertEqual(self._dfs(tree),
                         [19, 43, 7, 13, 3, 5, 1, 2, 3, 4, 5, 6, 9, 11, 7, 8, 9, 10, 11, 12, 15, 17, 13, 14, 15, 16, 17,
                          18, 29, 37, 21, 25, 27, 19, 20, 21, 22, 24, 25, 26, 27, 28, 32, 35, 29, 30, 32, 33, 34, 35,
                          36, 39, 41, 37, 38, 39, 40, 41, 42, 49, 55, 61, 45, 47, 43, 44, 45, 46, 47, 48, 51, 53, 49,
                          50, 51, 52, 53, 54, 57, 59, 55, 56, 57, 58, 59, 60, 63, 65, 67, 69, 61, 62, 63, 64, 65, 66,
                          67, 68, 69, 70, 71, 72])

        # when
        tree.delete(43)

        # then
        self.assertEqual(self._dfs(tree),
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
        self.assertEqual(self._dfs(tree),
                         [19, 7, 13, 3, 5, 1, 2, 3, 4, 5, 6, 9, 11, 7, 8, 9, 10, 11, 12, 15, 17, 13, 14, 15, 16, 17, 18,
                          29, 37, 47, 61, 21, 25, 27, 19, 20, 21, 22, 24, 25, 26, 27, 28, 32, 35, 29, 30, 32, 33, 34,
                          35, 36, 39, 41, 37, 38, 39, 40, 41, 42, 51, 53, 56, 59, 47, 48, 50, 51, 52, 53, 54, 56, 57,
                          58, 59, 60, 63, 65, 67, 61, 62, 63, 64, 65, 66, 67, 68])

        # when
        tree.delete(13)

        # then
        self.assertEqual(self._dfs(tree),
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
        self.assertEqual(self._dfs(tree),
                         [29, 7, 20, 3, 5, 1, 2, 3, 4, 5, 6, 11, 15, 7, 8, 9, 11, 12, 15, 16, 18, 24, 27, 20, 21, 24,
                          26, 27, 28, 38, 63, 32, 35, 29, 30, 32, 34, 35, 36, 41, 50, 61, 38, 40, 41, 42, 50, 60, 61,
                          62, 65, 67, 63, 64, 65, 66, 67, 68])

        # when
        tree.delete(29)

        # then
        self.assertEqual(self._dfs(tree),
                         [30, 7, 20, 3, 5, 1, 2, 3, 4, 5, 6, 11, 15, 7, 8, 9, 11, 12, 15, 16, 18, 24, 27, 20, 21, 24,
                          26, 27, 28, 41, 63, 35, 38, 30, 32, 34, 35, 36, 38, 40, 50, 61, 41, 42, 50, 60, 61, 62, 65,
                          67, 63, 64, 65, 66, 67, 68])

        # when
        tree.delete(11)
        tree.delete(15)
        tree.delete(30)
        tree.delete(24)

        # then
        self.assertEqual(self._dfs(tree),
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
        self.assertEqual(self._dfs(tree),
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
        self.assertEqual(self._dfs(tree), [])

    def test_should_properly_remove_random_keys(self):
        # given
        tree = BTree(5)

        large_array = [i for i in range(10000)]
        large_array_as_set = set(large_array)
        random.shuffle(large_array)

        for k in large_array:
            tree.insert(k, "val")

        for i, k in enumerate(large_array):
            # when
            tree.delete(k)

            # then
            large_array_as_set.remove(k)
            self.assertEqual(tree.get_leafs(), sorted(large_array_as_set))

    def test_should_preserve_not_deleted_values(self):
        # given
        tree = BTree(5)

        large_array = [i for i in range(10000)]
        random.shuffle(large_array)

        for k in large_array:
            tree.insert(k, f"val{k}")

        # when
        for i, k in enumerate(large_array):
            if len(large_array) - i <= 100:
                break
            tree.delete(k)

        # then
        self.assertEqual(len(tree.get_leafs()), 100)

        # when
        for k in large_array[len(large_array) - 100:]:
            self.assertEqual(tree.find(k), f"val{k}")

    def test_should_work_for_generic_type(self):
        # given
        tree = BTree(3)

        # when
        tree.insert((4, 1), (1, 4))
        tree.insert((0, 3), (3, 0))
        tree.insert((0, 1), (1, 0))
        tree.insert((0, 2), (2, 0))
        tree.insert((1, 0), (0, 1))
        tree.insert((1, 1), (1, 1))

        # then
        self.assertEqual(tree.find((0, 1)), (1, 0))
        self.assertEqual(tree.find((4, 1)), (1, 4))
        self.assertEqual(tree.find((0, 3)), (3, 0))

        self.assertEqual(tree.get_leafs(), [(0, 1), (0, 2), (0, 3), (1, 0), (1, 1), (4, 1)])

    def _dfs(self, tree: BTree) -> typing.List[int]:
        dfs_container = []
        self._dfs_helper(tree.root, dfs_container)
        return dfs_container

    def _dfs_helper(self, node: BTreeNode, container: typing.List):
        if not node:
            return []
        container.extend(node.keys)
        for n in node.children:
            self._dfs_helper(n, container)
