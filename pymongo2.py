from os import path
from codecs import encode, decode
from pymongo import MongoClient

conn = MongoClient()
db = conn.foo

with open("text.txt") as f:
    for line in f:
        text = []    
        for word in line.strip().split():
          word = decode(word.strip(),'latin2','ignore')
          text.append(word)
        d = {}
        d['content'] = text
        db.corpus.insert(d)
