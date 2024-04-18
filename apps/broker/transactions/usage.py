from apps.broker.transactions.database import Database
from apps.broker.transactions.record import DbKey, DbRecord

db = Database()

tx_id = db.begin_transaction()
db.txn_insert(tx_id, DbRecord(DbKey("key"), "value"))
db.txn_commit(tx_id)
