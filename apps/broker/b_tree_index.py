import typing
from collections import defaultdict
from dataclasses import dataclass


@dataclass
class DeleteResult:
    new_first: typing.Optional[int]
    condition_of_tree_valid: bool = True
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
                self.children = (self.children[:insert_index] + maybe_new_node.children +
                                 self.children[insert_index + 1:])
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

        # we deleted from leaf, we are not a parent, we have to replace deleted element (if present) with the inorder successor
        self._replace_key_if_needed(key, delete_res.new_first)

        # child has not enough keys/children, so try to borrow from siblings
        if delete_res.leaf and not delete_res.condition_of_tree_valid:
            if i > 0 and self.children[i - 1]._has_enough_to_lend():
                # borrow right-most key from left child
                self.children[i].keys.insert(0, self.children[i - 1].keys.pop())
                # self.children[i].values.insert(0, self.children[i - 1].values.pop())
            elif i + 1 < len(self.children) and self.children[i + 1]._has_enough_to_lend():
                # borrow left-most key from right child
                self.children[i].keys.append(self.children[i + 1].keys.pop(0))
                # self.children[i].values.append(self.children[i + 1].values.pop(0))
            else:
                # we still have invalid child and have to merge
                if i > 0:
                    # merge with left child
                    new_keys = self.children[i - 1].keys + self.children[i].keys
                    self.children[i].keys = new_keys
                    self.children[i - 1].keys = []
                elif i + 1 < len(self.children):
                    # merge with right child
                    new_keys = self.children[i].keys + self.children[i + 1].keys
                    self.children[i].keys = new_keys
                    self.children[i + 1].keys = []
            delete_res.new_first = self._rearrange_keys_and_get_new_first()
        # try to borrow from sibling being grandfather
        if not delete_res.leaf and not delete_res.condition_of_tree_valid:
            if i > 0 and self.children[i - 1]._has_enough_to_lend():
                # borrow right-most key from left child
                self.children[i].children.insert(0, self.children[i - 1].children.pop())
                # it may happen that we need to only override the key or that we need to add a new one
                if self.children[i]._has_enough_keys():
                    self.children[i].keys[0] = self.keys[i - 1]
                else:
                    self.children[i].keys.insert(0, self.keys[i - 1])
                self.keys[i - 1] = self.children[i - 1].keys.pop()
            elif i + 1 < len(self.children) and self.children[i + 1]._has_enough_to_lend():
                # borrow left-most key from right child
                self.children[i].children.append(self.children[i + 1].children.pop(0))
                # it may happen that we need to only override the key or that we need to add a new one
                if self.children[i]._has_enough_keys():
                    self.children[i].keys[-1] = self.keys[i]
                else:
                    self.children[i].keys.append(self.keys[i])
                self.keys[i] = self.children[i + 1].keys.pop(0)
            else:
                # we still have the invalid child and have to merge
                if i > 0:
                    # merge with left child
                    new_children = self.children[i - 1].children + self.children[i].children
                    new_keys = self.children[i - 1].keys + [self.keys.pop(i - 1)] + self.children[i].keys
                    self.children[i - 1].children = new_children
                    self.children[i - 1].keys = new_keys
                    self.children.pop(i)
                elif i + 1 < len(self.children):
                    # merge with right child
                    new_children = self.children[i].children + self.children[i + 1].children
                    new_keys = self.children[i].keys + [self.keys.pop(i)] + self.children[i + 1].keys
                    self.children[i + 1].children = new_children
                    self.children[i + 1].keys = new_keys
                    self.children.pop(i)
                else:
                    print("Impossibru...")

        # we are a parent, we deleted from leaf and tried to restore the tree condition
        if delete_res.leaf:
            delete_res.condition_of_tree_valid = self._is_at_least_half_full() and all(c._is_at_least_half_full() for c in self.children)
        else:
            delete_res.condition_of_tree_valid = self._is_at_least_half_full()
        delete_res.leaf = False
        return delete_res

    def _replace_key_if_needed(self, old: int, new: int):
        for i in range(len(self.keys)):
            if self.keys[i] == old:
                self.keys[i] = new
                break

    def _rearrange_keys_and_get_new_first(self) -> typing.Optional[int]:
        new_children = []
        for c in self.children:
            assert isinstance(c, BTreeNodeLeaf)
            if not c.keys:
                if c.prev:
                    assert isinstance(c.prev, BTreeNodeLeaf)
                    c.prev.next = c.next
                if c.next:
                    assert isinstance(c.next, BTreeNodeLeaf)
                    c.next.prev = c.prev
                continue
            new_children.append(c)
        self.keys = []
        self.children = new_children
        # rearrange our keys
        for i in range(1, len(self.children)):
            self.keys.append(self.children[i].keys[0])
        if self.children:
            return self.children[0].keys[0]
        return None

    def _has_enough_to_lend(self):
        return len(self.keys) > self.max_keys // 2

    def _has_enough_keys(self):
        return len(self.keys) >= self.max_keys // 2

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

            left_child.next = right_child
            left_child.prev = self.prev
            right_child.prev = left_child
            right_child.next = self.next
            if self.prev:
                self.prev.next = left_child
            if self.next:
                self.next.prev = right_child

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
        if not self.keys:
            return DeleteResult(None, condition_of_tree_valid=False, leaf=True)
        return DeleteResult(self.keys[0], condition_of_tree_valid=False, leaf=True)

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
        if len(self.root.keys) in [0, 1] and len(self.root.children) == 1:
            self.root = self.root.children[0]
        elif not self.root.keys and not self.root.children:
            self.root = None

    def print(self):
        container: typing.Dict[int, typing.List[BTreeNode]] = defaultdict(list)
        self._dfs(self.root, 1, container)
        for level in sorted(container.keys()):
            for node in container[level]:
                node_keys = "|".join(map(str, node.keys))
                print(f'[{node_keys}]', end=' ')
            print()

    def print_leafs(self):
        if not self.root:
            return
        curr_node = self.root
        while curr_node.children:
            curr_node = curr_node.children[0]
        assert isinstance(curr_node, BTreeNodeLeaf)
        while curr_node:
            node_keys = "|".join(map(str, curr_node.keys))
            print(f'[{node_keys}]', end='->')
            curr_node = curr_node.next

    def get_leafs(self):
        sorted_keys = []
        if not self.root:
            return
        curr_node = self.root
        while curr_node.children:
            curr_node = curr_node.children[0]
        assert isinstance(curr_node, BTreeNodeLeaf)
        while curr_node:
            sorted_keys.extend(curr_node.keys)
            curr_node = curr_node.next
        return sorted_keys

    def _dfs(self, root: BTreeNode, level: int, container: typing.Dict[int, typing.List[BTreeNode]]):
        if not root:
            return
        container[level].append(root)
        for c in root.children:
            self._dfs(c, level + 1, container)


class NoSuchKeyException(Exception):
    def __init__(self, msg: str):
        super().__init__(msg)
