import os

from apps.broker.persistent_btree import PersBTree
from apps.broker.storage_engine import DbSlotPointer

dir_path = os.path.dirname(os.path.realpath(__file__))
test_db_file_path = os.path.join(dir_path, 'tree.txt')

if not os.path.exists(test_db_file_path):
    with open(test_db_file_path, 'w+') as file:
        pass

with open(test_db_file_path, 'r+b') as file:
    tree = PersBTree(file, 4)
    tree.insert(5, DbSlotPointer(0, 0))
    # tree.insert(2, DbSlotPointer(0, 0))
    # tree.insert(3, DbSlotPointer(0, 0))
    # tree.insert(4, DbSlotPointer(0, 0))
