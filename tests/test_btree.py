import typing
import unittest

from apps.broker.b_tree_index import BTree, BTreeNode


class TestBTree(unittest.TestCase):
    def setUp(self) -> None:
        self.tree = BTree()

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
        self.assertEqual(self._dfs(self.tree), [2, 2, 6, 0, 1, 0, 0, 0, 0, 0, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 5, 20, 6, 7, 10, 20, 100])

    def _dfs(self, tree: BTree) -> typing.List[int]:
        dfs_container = []
        self._dfs_helper(tree.root, dfs_container)
        return dfs_container

    def _dfs_helper(self, node: BTreeNode, container: typing.List):
        container.extend(node.keys)
        for n in node.children:
            self._dfs_helper(n, container)
