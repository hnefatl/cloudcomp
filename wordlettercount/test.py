#!/usr/bin/env python3

import sys
import pathlib
import pyspark
import re

if len(sys.argv) != 3:
    raise RuntimeError("Usage: test.py <access_key> <secret_key>")

access_key = sys.argv[1]
secret_key = sys.argv[2]

# TODO(kc506): Move into CLI, pass keys to spark-submit
def load_creds():
    path = pathlib.Path.home() / pathlib.Path(".aws/credentials")
    with path.open() as f:
        lines = f.readlines()
    access_key = None
    secret_key = None
    for line in lines:
        segs = line.strip().split("=")
        if len(segs) != 2:
            continue
        if segs[0] == "aws_access_key_id":
            access_key = segs[1]
        elif segs[0] == "aws_secret_access_key":
            secret_key = segs[1]
    return (access_key, secret_key)

print(f"Access Key: {access_key}, Secret Key: {secret_key}")

sc = pyspark.SparkContext("local", "WordLetterCount", pyFiles=[])
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)

text_file = sc.textFile("s3a://group8.foo/asdf")

def mapper(word):
    return [((word, "w"), 1)] + [((letter, "l"), 1) for letter in word]

results = (
    text_file.flatMap(lambda line: re.findall(r"\w+", line))
    .flatMap(mapper) # Add tags for words/letters along with initial counts
    .reduceByKey(lambda x, y: x + y) # Sum frequencies of each word/letter
    .sortBy(lambda x: x[1], False) # Sort by frequency
    .groupBy(lambda x: x[0][1]) # Group into words vs letters (sorted order is preserved)
    .mapValues(lambda x: map(lambda y: (y[0][0], y[1]), x)) # Remove the tag from the value, as we're now grouped by tag
    .mapValues(list) # Convert the results of groupBy from iterables to lists
    .collectAsMap() # Get words
)

def process():
    pass

print(results)
#results.saveAsTextFile("output.txt")