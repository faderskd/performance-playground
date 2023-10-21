from apps.broker.b_tree_index import BTree

tree = BTree(3)

tree.insert(10, "val12")
tree.insert(29, "val13")
tree.insert(40, "val14")
tree.insert(25, "val15")
tree.insert(0, "val16")
tree.insert(5, "val17")
tree.insert(60, "val18")
tree.insert(2, "val19")
tree.insert(15, "val19")
tree.insert(16, "val19")
tree.insert(1, "val19")
tree.insert(30, "val19")

# removing from left
tree.delete(10)
tree.delete(15)
tree.delete(0)
tree.delete(1)
tree.delete(60)
tree.delete(40)
tree.delete(5)

# removing from right
# tree.delete(40)
# tree.delete(60)
# tree.delete(29)
# tree.delete(25)
# tree.delete(30)

tree.print()
print("-------------------------------------")
# tree.print_leafs()
