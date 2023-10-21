import typing
import unittest

from apps.broker.b_tree_index import BTree, BTreeNode


class TestBTree(unittest.TestCase):
    def setUp(self) -> None:
        self.tree = BTree(3)

    def test_should_build_proper_tree(self):
        # given
        self.tree.insert(1, "va1")
        self.tree.insert(20, "val2")
        self.tree.insert(10, "val3")
        self.tree.insert(100, "val4")
        self.tree.insert(5, "val5")
        self.tree.insert(6, "val6")
        self.tree.insert(7, "val7")
        self.tree.insert(2, "val8")
        self.tree.insert(2, "val9")
        self.tree.insert(2, "val10")
        self.tree.insert(2, "val11")
        self.tree.insert(2, "val12")
        self.tree.insert(2, "val13")
        self.tree.insert(2, "val14")
        self.tree.insert(2, "val15")
        self.tree.insert(0, "val16")
        self.tree.insert(0, "val17")
        self.tree.insert(0, "val18")
        self.tree.insert(0, "val19")
        self.tree.insert(0, "val20")

        # expect
        self.assertEqual(self._dfs(self.tree),
                         [2, 2, 6, 0, 1, 0, 0, 0, 0, 0, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 5, 20, 6, 7, 10, 20, 100])

    def test_should_properly_delete_from_tree_leafs_and_borrow_from_siblings(self):
        # given
        self.tree.insert(10, "val1")
        self.tree.insert(29, "val2")
        self.tree.insert(40, "val3")
        self.tree.insert(25, "val4")
        self.tree.insert(0, "val5")
        self.tree.insert(5, "val6")
        self.tree.insert(60, "val7")
        self.tree.insert(2, "val8")
        self.tree.insert(15, "val9")
        self.tree.insert(16, "val10")
        self.tree.insert(1, "val11")

        # expect
        self.assertEqual(self._dfs(self.tree), [16, 2, 10, 0, 1, 2, 5, 10, 15, 29, 16, 25, 29, 40, 60])

        # when
        self.tree.delete(5)
        self.tree.delete(2)
        self.tree.delete(1)
        self.tree.delete(16)
        self.tree.delete(29)
        self.tree.delete(25)

        # then
        self.assertEqual(self._dfs(self.tree), [40, 10, 15, 0, 10, 15, 60, 40, 60])

    def test_should_properly_delete_from_tree_leafs_and_merge_with_siblings(self):
        # given
        self.tree.insert(10, "val1")
        self.tree.insert(29, "val2")
        self.tree.insert(40, "val3")
        self.tree.insert(25, "val4")
        self.tree.insert(0, "val5")
        self.tree.insert(5, "val6")
        self.tree.insert(60, "val7")
        self.tree.insert(2, "val8")
        self.tree.insert(15, "val9")
        self.tree.insert(16, "val10")
        self.tree.insert(1, "val11")

        # expect
        self.assertEqual(self._dfs(self.tree), [16, 2, 10, 0, 1, 2, 5, 10, 15, 29, 16, 25, 29, 40, 60])

        # when
        self.tree.delete(40)
        self.tree.delete(60)
        self.tree.delete(29)

        # then
        self.assertEqual(self._dfs(self.tree), [16, 2, 10, 0, 1, 2, 5, 10, 15, 25, 16, 25])

    def test_should_properly_delete_from_tree_indexes_and_borrow_from_left_siblings(self):
        # given
        self.tree.insert(10, "val1")
        self.tree.insert(29, "val2")
        self.tree.insert(40, "val3")
        self.tree.insert(25, "val4")
        self.tree.insert(0, "val5")
        self.tree.insert(5, "val6")
        self.tree.insert(60, "val7")
        self.tree.insert(2, "val8")
        self.tree.insert(15, "val9")
        self.tree.insert(16, "val10")
        self.tree.insert(1, "val11")
        self.tree.insert(30, "val12")

        # expect
        self.assertEqual(self._dfs(self.tree), [16, 2, 10, 0, 1, 2, 5, 10, 15, 29, 40, 16, 25, 29, 30, 40, 60])

        # when
        self.tree.delete(10)
        self.tree.delete(15)
        self.tree.delete(0)
        self.tree.delete(1)
        self.tree.delete(60)
        self.tree.delete(40)
        self.tree.delete(5)

        # then
        self.assertEqual(self._dfs(self.tree), [29, 16, 2, 16, 25, 30, 29, 30])

    def test_should_properly_delete_from_tree_from_both_sides_including_leafs_and_indexes(self):
        # given
        self.tree.insert(1, "val1")
        self.tree.insert(100, "val1")
        self.tree.insert(50, "val1")
        self.tree.insert(75, "val1")
        self.tree.insert(25, "val1")
        self.tree.insert(90, "val1")
        self.tree.insert(12, "val1")
        self.tree.insert(40, "val1")
        self.tree.insert(80, "val1")
        self.tree.insert(95, "val1")
        self.tree.insert(6, "val1")
        self.tree.insert(77, "val1")
        self.tree.insert(200, "val1")
        self.tree.insert(78, "val1")
        self.tree.insert(89, "val1")
        self.tree.insert(41, "val1")
        self.tree.insert(42, "val1")
        self.tree.insert(43, "val1")
        self.tree.insert(44, "val1")
        self.tree.insert(81, "val1")
        self.tree.insert(82, "val1")
        self.tree.insert(83, "val1")
        self.tree.insert(76, "val1")
        self.tree.insert(74, "val1")
        self.tree.insert(111, "val1")
        self.tree.insert(112, "val1")

        # when
        self.tree.delete(50)
        self.tree.delete(74)
        self.tree.delete(44)
        self.tree.delete(75)
        self.tree.delete(41)
        self.tree.delete(43)

        # then
        self.assertEqual(self._dfs(self.tree), [42, 78, 90, 25, 1, 6 ,12, 25, 40, 76, 42, 76, 77, 81, 83, 78, 80, 81, 82, 83, 89, 100, 112, 90, 95, 100, 111, 112, 200])

        # when
        self.tree.insert(50, "val1")
        self.tree.insert(48, "val2")
        self.tree.insert(49, "val3")

        self.tree.delete(25)
        self.tree.delete(40)
        self.tree.delete(6)
        self.tree.delete(1)
        self.tree.delete(111)
        self.tree.delete(90)
        self.tree.delete(78)
        self.tree.delete(112)
        self.tree.delete(95)

        # then
        self.assertEqual(self._dfs(self.tree), [49, 80, 100, 42, 12, 42, 48, 76, 49, 50, 76, 77, 81, 83, 80, 81, 82, 83, 89, 200, 100, 200])

        # when
        self.tree.delete(100)
        self.tree.insert(78, "val1")
        self.tree.insert(79, "val2")

        self.tree.delete(80)
        self.tree.delete(82)
        self.tree.delete(76)
        self.tree.delete(79)

        # then
        self.assertEqual(self._dfs(self.tree), [49, 78, 83, 42, 12, 42, 48, 77, 49, 50, 77, 81, 78, 81, 200, 83, 89, 200])

        # when
        self.tree.insert(60, "val1")
        self.tree.insert(61, "val1")
        self.tree.insert(62, "val1")
        self.tree.insert(63, "val1")
        self.tree.insert(64, "val1")
        self.tree.insert(65, "val1")

        self.tree.delete(49)
        self.tree.delete(64)

        # then
        self.assertEqual(self._dfs(self.tree), [78, 50, 65, 42, 12, 42, 48, 60, 62, 50, 60, 61, 62, 63, 77, 65, 77, 83, 81, 78, 81, 200, 83, 89, 200])

        # when
        self.tree.delete(65)
        self.tree.delete(50)

        # then
        self.assertEqual(self._dfs(self.tree), [78, 60, 62, 42, 12, 42, 48, 61, 60, 61, 77, 62, 63, 77, 83, 81, 78, 81, 200, 83, 89, 200])

        # when


    def _dfs(self, tree: BTree) -> typing.List[int]:
        dfs_container = []
        self._dfs_helper(tree.root, dfs_container)
        return dfs_container

    def _dfs_helper(self, node: BTreeNode, container: typing.List):
        container.extend(node.keys)
        for n in node.children:
            self._dfs_helper(n, container)
