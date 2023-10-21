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

    def test_should_properly_delete_from_tree_case1(self):
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

    def test_should_properly_delete_from_tree_case2(self):
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

    def _dfs(self, tree: BTree) -> typing.List[int]:
        dfs_container = []
        self._dfs_helper(tree.root, dfs_container)
        return dfs_container

    def _dfs_helper(self, node: BTreeNode, container: typing.List):
        container.extend(node.keys)
        for n in node.children:
            self._dfs_helper(n, container)
