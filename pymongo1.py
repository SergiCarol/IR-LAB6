from pymongo import MongoClient
import csv
from bson.code import Code

# server on localhost
conn = MongoClient()
db = conn.test    # db = conn.foo would connect to database 'foo'
db.test.drop()

support = dict()
confidence = dict()


def read_file(file):
    total_length = 0
    with open(file) as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for line in reader:
            d = dict()
            d['data'] = line
            db.test.insert_one(d)
            total_length += 1
    return total_length


def map_reduce():
    mapper_pairs = Code("""
        function() {
            for (var i = 0; i < this.data.length; i++) {
                for (var j = i+1; j <= this.data.length; j++){
                    emit(this.data[i] + "," + this.data[j], 1);
                    emit(this.data[j] + "," + this.data[i], 1);
                    }
                }
            }
            """)
    mapper_terms = Code("""
        function() {
            for (var i = 0; i < this.data.length; i++) {
                emit(this.data[i],1);
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
    db.test.map_reduce(mapper_terms, reducer, "count_terms")
    db.test.map_reduce(mapper_pairs, reducer, "count_pairs")


def associations(support, confidence, n):

    result_terms = db.count_pairs.find()
    for element in result_terms:
        print element


def calculate_associations(total_length):
    result_terms = db.count_terms.find()
    result_pairs = db.count_pairs.find()

    for element in result_terms:
        print element


if __name__ == '__main__':
    total_length = read_file('groceries.csv')
    print total_length
    map_reduce()
    calculate_associations(total_length) 