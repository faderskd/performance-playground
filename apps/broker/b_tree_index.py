import typing
from collections import defaultdict
from dataclasses import dataclass

MAX_KEYS = 3


@dataclass
class DeleteResult:
    new_first: typing.Optional[int]


class BTreeNode:
    def __init__(self):
        self.children: typing.List[BTreeNode] = []
        self.keys: typing.List[int] = []

    def insert(self, key: int, value: str) -> typing.Optional['BTreeNode']:
        for i in range(len(self.keys)):
            if self.keys[i] >= key:
                maybe_new_node = self.children[i].insert(key, value)
                break
        else:
            maybe_new_node = self.children[-1].insert(key, value)

        if maybe_new_node:
            first_key = maybe_new_node.keys[0]
            for i in range(len(self.keys)):
                if self.keys[i] >= first_key:
                    self.keys.insert(i, first_key)
                    self.children = self.children[:i] + maybe_new_node.children + self.children[i + 1:]
                    break
            else:
                self.keys.append(first_key)
                self.children.pop()
                self.children.extend(maybe_new_node.children)

        if len(self.keys) > MAX_KEYS:
            mid = (len(self.keys) + 1) // 2
            child_mid = (len(self.children) + 1) // 2
            left_keys, right_keys = self.keys[:mid], self.keys[mid + 1:]
            left_children, right_children = self.children[:child_mid], self.children[child_mid:]
            left_child = BTreeNode()
            left_child.keys = left_keys
            left_child.children = left_children
            right_child = BTreeNode()
            right_child.keys = right_keys
            right_child.children = right_children
            parent = BTreeNode()
            parent.keys = [self.keys[mid]]
            parent.children = [left_child, right_child]
            return parent

    def delete(self, key: int) -> typing.Optional[DeleteResult]:
        for i in range(len(self.keys)):
            if self.keys[i] > key:
                delete_res = self.children[i].delete(key)
                if delete_res:
                    if i > 0 and self.keys[i - 1] == key:
                        self.keys[i - 1] = delete_res.new_first
                    break
        else:
            delete_res = self.children[-1].delete(key)
            if delete_res and self.keys[-1] == key:
                self.keys[-1] = delete_res.new_first

        # TODO: handle rebalance
        return delete_res

    def __repr__(self):
        return str(self.keys)


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
            mid = (len(self.keys) + 1) // 2
            left_keys, right_keys = self.keys[:mid], self.keys[mid:]
            left_values, right_values = self.values[:mid], self.values[mid:]
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

    def delete(self, key: int) -> typing.Optional[DeleteResult]:
        for i in range(len(self.keys)):
            if self.keys[i] == key:
                self.keys.pop(i)
                break
        else:
            raise NoSuchKeyException(f'No key {key} found in a tree')

        half_full = len(self.keys) >= MAX_KEYS // 2
        if not half_full:
            pass  # TODO: handle rebalance
        return DeleteResult(self.keys[0])

    def __repr__(self):
        return str(self.keys)


class BTree:
    def __init__(self):
        self.root = BTreeNodeLeaf()
        self.height = 1

    def insert(self, key: int, value: str):
        maybe_new_root = self.root.insert(key, value)
        if maybe_new_root:
            self.root = maybe_new_root
            self.height += 1

    def delete(self, key: int):
        self.root.delete(key)

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


class NoSuchKeyException(Exception):
    def __init__(self, msg: str):
        super().__init__(msg)
