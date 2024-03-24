from apps.broker.transactions.database import Database, DbRecord, DbKey

db = Database()

tx_id = db.begin_transaction()
db.txt_insert(tx_id, DbRecord(DbKey("key"), "value"))
db.commit(tx_id)
