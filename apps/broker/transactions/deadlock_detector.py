import collections
import dataclasses
import enum
from typing import List, Set, Dict, Optional

from apps.broker.transactions.record import DbKey
from apps.broker.transactions.transaction import TxnId


class OpType(enum.Enum):
    READ = 0
    WRITE = 1


@dataclasses.dataclass(frozen=True)
class TxnOperation:
    op: OpType
    txn_id: TxnId


@dataclasses.dataclass(frozen=True)
class IndexItem:
    txn_id: TxnId
    operation_rows: Set[DbKey]
    graph_rows: Set[TxnId]


@dataclasses.dataclass(frozen=True)
class DetectedCycleSupersetStack:
    first_in_cycle: TxnId
    stack: Set[TxnId]


class DeadlockDetector:
    def __init__(self):
        self._index: Dict[TxnId, IndexItem] = {}
        self._operations: Dict[DbKey, List[TxnOperation]] = collections.defaultdict(list)
        self._graph: Dict[TxnId, Set[TxnOperation]] = {}

    def add_read(self, txn_id: TxnId, key: DbKey):
        if txn_id not in self._index:
            self._index[txn_id] = IndexItem(txn_id, set(), set())

        for operation in self._operations[key]:
            if operation.txn_id != txn_id:
                if txn_id not in self._graph:
                    self._graph[txn_id] = set()
                if operation.op != OpType.READ:
                    self._graph[txn_id].add(operation)
                    self._index[operation.txn_id].graph_rows.add(txn_id)
        self._operations[key].append(TxnOperation(OpType.READ, txn_id))
        self._index[txn_id].operation_rows.add(key)

    def add_write(self, txn_id: TxnId, key: DbKey):
        if txn_id not in self._index:
            self._index[txn_id] = IndexItem(txn_id, set(), set())

        for operation in self._operations[key]:
            if operation.txn_id != txn_id:
                if txn_id not in self._graph:
                    self._graph[txn_id] = set()
                self._graph[txn_id].add(operation)
                self._index[operation.txn_id].graph_rows.add(txn_id)
        self._operations[key].append(TxnOperation(OpType.WRITE, txn_id))
        self._index[txn_id].operation_rows.add(key)

    def remove_txn(self, txn_id: TxnId):
        self._graph.pop(txn_id)
        for db_key in self._index[txn_id].operation_rows:
            self._operations[db_key] = [op for op in self._operations[db_key] if op.txn_id != txn_id]
            if len(self._operations[db_key]) == 0:
                del self._operations[db_key]
        for related_txn_id in self._index[txn_id].graph_rows:
            self._graph[related_txn_id] = {op for op in self._graph[related_txn_id] if op.txn_id != txn_id}
            if len(self._graph[related_txn_id]) == 0:
                del self._graph[related_txn_id]
        self._index.pop(txn_id)

    def remove_key(self):
        pass

    def detect(self) -> List[Set[TxnId]]:
        """
        Returns a list of transactions that should be killed
        T1           T2          T3
        R_A          R_B        R_C
        W_B          W_C        W_A


        index = {
            T1: ([opearation_rows], [transaction_rows]),
            T2: ([opearation_rows], [transaction_rows]),
            ...
        }

        operations
        {
         A: R_T1, W_T3
         B: R_T2, W_T1
         C: R_T3, W_T2
        }

        transactions
        {
          T1 -> T2
          T2 -> T3
          T3 -> T1
        }
        """
        all_cyclic_transactions: List[Set[TxnId]] = []
        visited = set()
        for t in self._graph.keys():
            if t in visited:
                continue
            stack = set()
            if detected := self._detect_recursive(stack, visited, t):
                all_cyclic_transactions.append(self._find_correct_cycle_recursive(detected.first_in_cycle, set()))
        return all_cyclic_transactions

    def _detect_recursive(self, stack: Set[TxnId], visited: Set[TxnId],
                          txn_id: TxnId) -> Optional[DetectedCycleSupersetStack]:
        stack.add(txn_id)
        visited.add(txn_id)
        for op in self._graph[txn_id]:
            if op.txn_id in stack:
                return DetectedCycleSupersetStack(op.txn_id, stack)
            if op.txn_id in visited:
                continue
            if detected := self._detect_recursive(stack, visited, op.txn_id):
                return detected
        stack.remove(txn_id)
        return None

    def _find_correct_cycle_recursive(self, curr_txn_id: TxnId,
                                      correct_stack: Set[TxnId]) -> Optional[Set[TxnId]]:
        correct_stack.add(curr_txn_id)
        for op in self._graph[curr_txn_id]:
            if op.txn_id in correct_stack:
                return correct_stack
            if ret := self._find_correct_cycle_recursive(op.txn_id, correct_stack):
                return ret
        return None
