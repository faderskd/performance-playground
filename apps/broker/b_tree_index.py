import typing
from collections import defaultdict

MAX_KEYS = 5


class BTreeNode:
    def __init__(self):
        self.children: typing.List[BTreeNode] = []
        self.keys: typing.List[int] = []


class BTreeNodeLeaf(BTreeNode):
    def __init__(self):
        super().__init__()
        self.values: typing.List[str] = []

    def insert(self, key: int, value: str) -> typing.Optional[BTreeNode]:
        for i in range(len(self.keys)):
            if self.keys[i] > key:
                self.keys.insert(i, key)
                self.values.insert(i, value)
                break
        else:
            self.keys.append(key)
            self.values.append(value)

        if len(self.keys) > MAX_KEYS:
            mid = len(self.keys) // 2
            left_keys, right_keys = self.keys[:mid], self.keys[mid:]
            left_values, right_values = self.keys[:mid], self.keys[mid:]
            left_child = BTreeNodeLeaf()
            left_child.keys = left_keys
            left_child.values = left_values
            right_child = BTreeNodeLeaf()
            right_child.keys = right_keys
            right_child.values = right_values
            parent = BTreeNode()
            parent.keys = [self.keys[mid]]
            parent.children = [left_child, right_child]
            return parent


class BTree:
    def __init__(self):
        self.root = BTreeNodeLeaf()
        self.height = 1

    def insert(self, key: int, value: str):
        maybe_new_root = self.root.insert(key, value)
        if maybe_new_root:
            self.root = maybe_new_root
            self.height += 1

    def print(self):
        container: typing.Dict[int, typing.List[BTreeNode]] = defaultdict(list)
        self._dfs(self.root, 1, container)
        for level in sorted(container.keys()):
            for node in container[level]:
                node_keys = "|".join(map(str, node.keys))
                print(f'[{node_keys}]', end=' ')
            print()

    def _dfs(self, root: BTreeNode, level: int, container: typing.Dict[int, typing.List[BTreeNode]]):
        container[level].append(root)
        for c in root.children:
            self._dfs(c, level + 1, container)
