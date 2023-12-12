import os
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
        self.assertEqual(tree.dfs(), [40, 10, 15, 0, 10, 15, 60, 40, 60])
