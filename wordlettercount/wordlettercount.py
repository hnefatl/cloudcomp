#!/usr/bin/env python3

import sys
import os
import pyspark

try:
    import psycopg2
except ImportError:
    print(os.environ["PYTHONPATH"])
import re

if len(sys.argv) != 8:
    raise RuntimeError(
        "Usage: test.py <db_host> <db_port> <db_user> <db_pass> <db_name> <access_key> <secret_key>"
    )

db_host = sys.argv[1]
db_port = sys.argv[2]
db_user = sys.argv[3]
db_pass = sys.argv[4]
db_name = sys.argv[5]
access_key = sys.argv[6]
secret_key = sys.argv[7]

print(f"Access Key: {access_key}, Secret Key: {secret_key}")

sc = pyspark.SparkContext("local", "WordLetterCount", pyFiles=[])
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)

text_file = sc.textFile("s3a://group8.foo/asdf")


def mapper(word):
    return [((word, "w"), 1)] + [((letter, "l"), 1) for letter in word]


results = (
    text_file.flatMap(lambda line: re.findall(r"\w+", line))
    .flatMap(mapper)  # Add tags for words/letters along with initial counts
    .reduceByKey(lambda x, y: x + y)  # Sum frequencies of each word/letter
    .sortBy(lambda x: x[1], False)  # Sort by frequency
    .groupBy(
        lambda x: x[0][1]
    )  # Group into words vs letters (sorted order is preserved)
    .mapValues(
        lambda x: map(lambda y: (y[0][0], y[1]), x)
    )  # Remove the tag from the value, as we're now grouped by tag
    .mapValues(list)  # Convert the results of groupBy from iterables to lists
    .collectAsMap()  # Get words
)


def process():
    pass


def write_to_db(host, port, db_name, user, password, data):
    print(f"Connecting to {host}:{port}")
    conn = psycopg2.connect(
        f"host='{host}' port='{port}' dbname='{db_name}' user='{user}' password='{password}'"
    )
    cursor = conn.cursor()
    print("Deleting old tables")
    cursor.execute("DROP TABLE IF EXISTS words_spark")
    cursor.execute("DROP TABLE IF EXISTS letters_spark")
    print("Creating new tables")
    cursor.execute(
        "CREATE TABLE words_spark ( rank INTEGER PRIMARY KEY, word TEXT NOT NULL, category TEXT NOT NULL, frequency INTEGER NOT NULL )"
    )
    cursor.execute(
        "CREATE TABLE letters_spark ( rank INTEGER PRIMARY KEY, letter TEXT NOT NULL, category TEXT NOT NULL, frequency INTEGER NOT NULL )"
    )


print(results)
# results.saveAsTextFile("output.txt")
