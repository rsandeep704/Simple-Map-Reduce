import MapReduce
import sys
import re
import json


mr = MapReduce.MapReduce()
json.JSONEncoder.key_separator = ','


def mapper(record):
    # key: document identifier
    # value: document contents
    
    key = record[0].lower()
    value = record[1].lower()
    words = value.split()
    for w in words:
        if re.match((r'[A-Za-z0-9]+'), w):
            occur = len(re.findall(r"" + re.escape(w), re.escape(value)))
            mr.emit_intermediate(w, (key, occur))


def reducer(key, list_of_values):
    total = 0
    x = []
    for v in list_of_values:
        if v not in x:
            x.append(v)
            total += 1
    mr.emit((key, total, x))


inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
