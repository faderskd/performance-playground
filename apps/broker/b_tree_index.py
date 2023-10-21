import typing
from collections import defaultdict
from dataclasses import dataclass


@dataclass
class DeleteResult:
    new_first: typing.Optional[int]
    not_enough_keys: bool = False
    leaf: bool = False


class BTreeNode:
    def __init__(self, max_keys: int):
        self.children: typing.List[BTreeNode] = []
        self.keys: typing.List[int] = []
        self.values: typing.Optional[typing.List[str]] = None  # None in non-leaf nodes
        self.max_keys = max_keys

    def insert(self, key: int, value: str) -> typing.Optional['BTreeNode']:
        for i in range(len(self.keys)):
            if self.keys[i] >= key:
                maybe_new_node = self.children[i].insert(key, value)
                insert_index = i
                break
        else:
            maybe_new_node = self.children[-1].insert(key, value)
            insert_index = len(self.children)

        if maybe_new_node:
            first_key = maybe_new_node.keys[0]
            self.keys.insert(insert_index, first_key)
            if insert_index < len(self.children):
                self.children = self.children[:insert_index] + maybe_new_node.children + self.children[
                                                                                         insert_index + 1:]
            else:
                self.children.pop()
                self.children.extend(maybe_new_node.children)

        if len(self.keys) > self.max_keys:
            mid = len(self.keys) // 2
            child_mid = (len(self.children) + 1) // 2
            left_keys, right_keys = self.keys[:mid], self.keys[mid + 1:]
            left_children, right_children = self.children[:child_mid], self.children[child_mid:]
            left_child = BTreeNode(self.max_keys)
            left_child.keys = left_keys
            left_child.children = left_children
            right_child = BTreeNode(self.max_keys)
            right_child.keys = right_keys
            right_child.children = right_children
            parent = BTreeNode(self.max_keys)
            parent.keys = [self.keys[mid]]
            parent.children = [left_child, right_child]
            return parent

    def delete(self, key: int) -> typing.Optional[DeleteResult]:
        # search for key to delete
        for i in range(len(self.keys)):
            if self.keys[i] > key:
                delete_res = self.children[i].delete(key)
                break
        else:
            i = len(self.keys)
            delete_res = self.children[i].delete(key)

        if not delete_res:
            return

        # we are parent, child has not enough keys, so try to borrow from siblings
        if delete_res.leaf and delete_res.not_enough_keys:
            if i > 0 and self.children[i - 1]._has_enough_to_lend():
                # borrow right-most key from left child
                self.children[i].keys.insert(0, self.children[i - 1].keys.pop())
                self.children[i].values.insert(0, self.children[i - 1].values.pop())
            elif i + 1 < len(self.children) and self.children[i + 1]._has_enough_to_lend():
                # borrow left-most key from right child
                self.children[i].keys.append(self.children[i + 1].keys.pop(0))
                self.children[i].values.append(self.children[i + 1].values.pop(0))
            else:
                # we have empty child !
                pass

        # we are parent, we deleted from leaf and try to remove any empty child
        if delete_res.leaf:
            self.children = [c for c in self.children if c.keys]
            self.keys = []
            # rearrange our keys
            for i in range(1, len(self.children)):
                self.keys.append(self.children[i].keys[0])
            if self.children:
                delete_res.new_first = self.children[0].keys[0]

        # we are parent, we deleted from leaf, tried to borrow, but it didn't help, we rearranged the keys and it is still bad
        if not self._is_at_least_half_full():
            delete_res.not_enough_keys = True
        # condition of the b+tree is maintained
        else:
            delete_res.not_enough_keys = False



        # we deleted from leaf, we are not a parent, we have to replace deleted element (if present) with the inorder successor
        if not delete_res.leaf and not delete_res.not_enough_keys:
            for i in range(len(self.keys)):
                if self.keys[i] == key:
                    self.keys[i] = delete_res.new_first


        delete_res.leaf = False
        return delete_res

    def _has_enough_to_lend(self):
        return len(self.keys) > self.max_keys // 2

    def _is_at_least_half_full(self):
        return len(self.keys) >= self.max_keys // 2 and len(self.children) > self.max_keys // 2

    def __repr__(self):
        return str(self.keys)


class BTreeNodeLeaf(BTreeNode):
    def __init__(self, max_keys: int):
        super().__init__(max_keys)
        self.next: typing.Optional[BTreeNode] = None
        self.prev: typing.Optional[BTreeNode] = None
        self.values = []

    def insert(self, key: int, value: str) -> typing.Optional[BTreeNode]:
        for i in range(len(self.keys)):
            if self.keys[i] > key:
                self.keys.insert(i, key)
                self.values.insert(i, value)
                break
        else:
            self.keys.append(key)
            self.values.append(value)

        if len(self.keys) > self.max_keys:
            mid = len(self.keys) // 2
            left_keys, right_keys = self.keys[:mid], self.keys[mid:]
            left_values, right_values = self.values[:mid], self.values[mid:]
            left_child = BTreeNodeLeaf(self.max_keys)
            left_child.keys = left_keys
            left_child.values = left_values
            right_child = BTreeNodeLeaf(self.max_keys)
            right_child.keys = right_keys
            right_child.values = right_values

            # left_child.next = right_child
            # left_child.prev = self.prev
            # right_child.prev = left_child
            # right_child.next = self.next
            # if self.prev:
            #     self.prev.next = left_child
            # if self.next:
            #     self.next.prev = right_child

            parent = BTreeNode(self.max_keys)
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

        if self._is_at_least_half_full():
            # case when after deletion b+tree condition is maintained in leaf, nothing to do more
            return DeleteResult(self.keys[0], leaf=True)
        # case when there is not enough elements in leaf after deletion
        if self.keys:
            return DeleteResult(self.keys[0], not_enough_keys=True, leaf=True)
        return DeleteResult(None, not_enough_keys=True, leaf=True)

    def __repr__(self):
        return str(self.keys)

    def _is_at_least_half_full(self):
        return len(self.keys) >= self.max_keys // 2

class BTree:
    def __init__(self, max_keys: int):
        self.root = BTreeNodeLeaf(max_keys)
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

    def print_leafs(self):
        curr_node = self.root
        while curr_node.children:
            curr_node = curr_node.children[0]
        assert isinstance(curr_node, BTreeNodeLeaf)
        while curr_node:
            node_keys = "|".join(map(str, curr_node.keys))
            print(f'[{node_keys}]', end='->')
            curr_node = curr_node.next

    def _dfs(self, root: BTreeNode, level: int, container: typing.Dict[int, typing.List[BTreeNode]]):
        container[level].append(root)
        for c in root.children:
            self._dfs(c, level + 1, container)


class NoSuchKeyException(Exception):
    def __init__(self, msg: str):
        super().__init__(msg)
