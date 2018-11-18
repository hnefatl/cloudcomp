#!/usr/bin/env python3

import pathlib
import pyspark
import re

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

access_key, secret_key = load_creds()
print(f"Access Key: {access_key}, Secret Key: {secret_key}")

sc = pyspark.SparkContext("local", "WordLetterCount", pyFiles=[])
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)

text_file = sc.textFile("s3a://group8.foo/asdf")

results = (
    text_file.flatMap(lambda line: re.findall(r"\w+", line))
    .map(lambda word: (word, 1))
    .reduceByKey(lambda x, y: x + y)
    .sortBy(lambda x: x[1], False)
)

results.saveAsTextFile("output.txt")
