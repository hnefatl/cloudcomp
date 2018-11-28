#!/usr/bin/env python3

import sys
import pyspark
import time
from math import ceil, floor
import re

from common import db


def is_ascii_alpha(word):
    # string isalpha method allows unicode, which we don't want
    return all(c >= "a" and c <= "z" for c in word)


def mapper(word):
    wl = word.lower()
    wordlist = [((wl, "w"), 1)] if is_ascii_alpha(wl) and len(wl) > 0 else []
    return wordlist + [((letter, "l"), 1) for letter in wl if is_ascii_alpha(letter)]


delims = list(' \n\t\r\v\f,.;:?!"()[]{}-_')
re_split = re.compile(r"|".join(map(re.escape, delims)))


def process(data):
    def process_categories(subdata):
        length = len(subdata)
        # Category boundaries. In the sample outputs, the ranges are inclusive on both sides.
        # Have to subtract one from the lower bounds to get the same output
        popular_u = int(ceil(0.05 * length))
        common_l = int(floor(0.475 * length)) - 1
        common_u = int(ceil(0.525 * length))
        rare_l = int(floor(0.95 * length)) - 1

        for r in range(0, popular_u):
            yield (r + 1, subdata[r][0], "popular", subdata[r][1])
        for r in range(common_l, common_u):
            yield (r + 1, subdata[r][0], "common", subdata[r][1])
        for r in range(rare_l, length):
            yield (r + 1, subdata[r][0], "rare", subdata[r][1])

    return (process_categories(data["w"]), process_categories(data["l"]))


def write_to_db(host, port, db_name, username, password, data):
    word_data, letter_data = process(data)

    print(f"Connecting to {host}:{port}")
    with db.pymysql_connect(
        host=host, port=port, user=username, password=password, db=db_name
    ) as connection, connection.cursor() as cursor:
        cursor.executemany("INSERT INTO words_spark VALUES (%s,%s,%s,%s)", word_data)
        cursor.executemany(
            "INSERT INTO letters_spark VALUES (%s,%s,%s,%s)", letter_data
        )
        connection.commit()


if __name__ == "__main__":
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
    if input_url.startswith("s3://"):
        input_url = f"s3a{input_url[2:]}"
    if not input_url.startswith("s3a://"):
        raise RuntimeError("input_file should be an s3:// or s3a:// url.")

    print(f"Access Key: {access_key}, Secret Key: {secret_key}")

    sc = pyspark.SparkContext(appName="WordLetterCount")
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)

    start_time_s = time.monotonic()
    text_file = sc.textFile(input_url)

    results = (
        text_file.flatMap(lambda line: re_split.split(line))  # Split on punctuation
        .flatMap(mapper)  # Add tags for words/letters along with initial counts
        .reduceByKey(lambda x, y: x + y)  # Sum frequencies of each word/letter
        .groupBy(
            lambda x: x[0][1]
        )  # Group words vs letters (sorted order is preserved)
        .mapValues(
            lambda x: map(lambda y: (y[0][0], y[1]), x)
        )  # Remove tag from the value
        .mapValues(list)
        .collectAsMap()  # Get dictionary of words/letters
    )
    # Sort the resulting lists of pairs for words/letters
    for r in results:
        results[r].sort(key=lambda x: x[0])
        results[r].sort(key=lambda x: x[1], reverse=True)

    write_to_db(
        host=db_host,
        port=int(db_port),
        db_name=db_name,
        username=db_user,
        password=db_pass,
        data=results,
    )
    end_time_s = time.monotonic()
    print(f"Took {end_time_s - start_time_s}s")
