#!/usr/bin/env python3

import contextlib
import sys
import os
import pyspark
import pymysql
import re

if len(sys.argv) != 9:
    raise RuntimeError(
        "Usage: test.py <access_key> <secret_key> <db_host> <db_port> <db_user> <db_pass> <db_name> <input_file>\n"
        + f"Got: {sys.argv[1:]}"
    )

access_key = sys.argv[1]
secret_key = sys.argv[2]
db_host = sys.argv[3]
db_port = sys.argv[4]
db_user = sys.argv[5]
db_pass = sys.argv[6]
db_name = sys.argv[7]
input_url = sys.argv[8]
if not input_url.startswith("s3a://"):
    raise RuntimeError("input_file should be an s3a:// url.")

print(f"Access Key: {access_key}, Secret Key: {secret_key}")

sc = pyspark.SparkContext("local", "WordLetterCount")
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)

text_file = sc.textFile(input_url)


def mapper(word):
    return [((word, "w"), 1)] + [((letter, "l"), 1) for letter in word]


results = (
    text_file.flatMap(lambda line: re.findall(r"\w+", line))
    .flatMap(mapper)  # Add tags for words/letters along with initial counts
    .reduceByKey(lambda x, y: x + y)  # Sum frequencies of each word/letter
    .sortBy(lambda x: x[1], False)  # Sort by frequency
    .groupBy(lambda x: x[0][1])  # Group words vs letters (sorted order is preserved)
    .mapValues(lambda x: map(lambda y: (y[0][0], y[1]), x))  # Remove tag from the value
    .mapValues(list)  # Convert the results of groupBy from iterables to lists
    .collectAsMap()  # Get words
)


def process(data):
    def process_categories(subdata):
        length = len(subdata)
        output = []
        for r in range(0, int(0.05 * length)):
            output.append((r + 1, subdata[r][0], "popular", subdata[r][1]))
        for r in range(int(0.475 * length), int(0.525 * length)):
            output.append((r + 1, subdata[r][0], "common", subdata[r][1]))
        for r in range(int(0.95 * length), length):
            output.append((r + 1, subdata[r][0], "rare", subdata[r][1]))
        return output

    return (process_categories(data["w"]), process_categories(data["l"]))


# Copied from rds.py to remove the dependency
class pymysql_connect:
    def __init__(self, *args, **kwargs):
        self._conn = pymysql.connect(*args, **kwargs)
        self._exitstack = contextlib.ExitStack()

    def __enter__(self):
        self._exitstack.enter_context(self._conn)
        return self._conn

    def __exit__(self, *_):
        self._exitstack.close()
        self._conn.close()

def write_to_db(host, port, db_name, username, password, data):
    word_data, letter_data = process(data)

    print(f"Connecting to {host}:{port}")
    with pymysql_connect(
        host=host, port=port, user=username, password=password, db=db_name
    ) as connection, connection.cursor() as cursor:
        cursor.executemany("INSERT INTO words_spark VALUES (%s,%s,%s,%s)", word_data)
        cursor.executemany("INSERT INTO letters_spark VALUES (%s,%s,%s,%s)", letter_data)
        connection.commit()


print(results)
write_to_db(
    host=db_host,
    port=int(db_port),
    db_name=db_name,
    username=db_user,
    password=db_pass,
    data=results,
)
# results.saveAsTextFile("output.txt")
