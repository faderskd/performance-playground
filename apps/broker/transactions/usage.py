from apps.broker.transactions.database import Database, DbRecord, DbKey

db = Database()

# for i in range(10000):
#     db.insert(DbRecord(DbKey(f"key{i}"), f"val{i}"))

print(db.read(DbKey("key100")))
