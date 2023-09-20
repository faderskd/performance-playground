
from apps.broker.b_tree_index import BTree

tree = BTree()
tree.insert(10, "key1")
tree.insert(4, "key1")
tree.insert(0, "key1")
tree.insert(7, "key1")
tree.insert(2, "key1")
tree.insert(2, "key1")

tree.print()
