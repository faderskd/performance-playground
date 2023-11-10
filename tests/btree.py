import random
import typing

from apps.broker.b_tree_index import BTree, BTreeNode


def dfs(tree: BTree) -> typing.List[int]:
    dfs_container = []
    dfs_helper(tree.root, dfs_container)
    return dfs_container


def dfs_helper(node: BTreeNode, container: typing.List):
    if not node:
        return []
    container.extend(node.keys)
    for n in node.children:
        dfs_helper(n, container)


tree = BTree(4)

large_array = [i for i in range(100000)]
large_array_as_set = set(large_array)
random.shuffle(large_array)

for k in large_array:
    tree.insert(k, "val")

random.shuffle(large_array)

for i, k in enumerate(large_array):
    if len(large_array) - i < 10:
        break
    tree.delete(k)
    large_array_as_set.remove(k)

assert tree.get_leafs() == sorted(large_array_as_set)
tree.print()
