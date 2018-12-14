from pymongo import MongoClient

# server on localhost
conn = MongoClient()
db = conn.test    # db = conn.foo would connect to database 'foo'

db.testing.insert({"j":5, "k":6, "m":"nopqr" })
db.testing.insert({"j":15, "k":6, "m":"nopqr" })

d = {}
d['j'] = 10
d['k'] = 6
for i in range(4):
    d['a'*i] = i
db.testing.insert(d)

d = {}
d['j'] = 8
d['k'] = 6
d['b'] = "Hi there!"
db.testing.insert(d)

docs = db.testing.find({"j":10})

for doc in docs:
    print "(j:10)", doc

for doc in db.testing.find({"k":{"$lt":7}}):
    print "(k<7)", doc

for doc in db.testing.find({"j":{"$gt":9}}):
    print "(j>9)", doc

db.testing.remove({ "k" : 6 })
