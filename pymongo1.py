from pymongo import MongoClient
import csv
from codecs import encode, decode
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
                for (var j = i+1; j < this.data.length; j++){
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


def associations(supconf):
    counter = []
    for sup, conf in supconf:
        c = 0
        for element in support.keys():
            if support[element] > sup and confidence[element] > conf:
                c += 1
        counter.append("Support: %d, Coinfidence %d, Associations: %d" % (sup, conf, c))
    return counter


def calculate_associations(total_length):
    result_pairs = db.count_pairs.find()

    for element in result_pairs:
        support[element['_id']] = float(element['value'] * 100 / total_length)
        x = element['_id'].split(',')[0]
        result_terms = db.count_terms.find({'_id': x})[0]
        confidence[element['_id']] = float(
            element['value'] * 100 / result_terms['value'])


if __name__ == '__main__':
    total_length = read_file('groceries.csv')
    map_reduce()
    calculate_associations(total_length)
    items = [(1, 1), (1, 25), (1, 50), (1, 75), (5, 25), (7, 25),
             (20, 25), (50, 25)]
    for element in associations(items):
        print element
