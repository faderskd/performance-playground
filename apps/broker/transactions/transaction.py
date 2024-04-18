from dataclasses import dataclass


@dataclass(frozen=True)
class TxnId:
    id: int


class TxnIdGenerator:
    def __init__(self):
        self._txn_id = -1

    def generate(self) -> TxnId:
        self._txn_id += 1
        return TxnId(self._txn_id)


class InvalidTransactionId(RuntimeError):
    def __init__(self, msg):
        super().__init__(msg)
