from pymongo import MongoClient
from bson.code import Code

conn = MongoClient()
db = conn.foo

mapper = Code("""
function() {
    for (var i = 0; i < this.content.length; i++) {
        emit(this.content[i],1);
        }
    }
              """)

reducer = Code("""
function(key,values) {
    var total = 0;
    for (var i = 0; i < values.length; i++) {
        total += values[i];
        }
        return total;
    }
               """)  

r = db.corpus.map_reduce(mapper, reducer, "counts")
