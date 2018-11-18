#!/usr/bin/env python3

import pyspark
import re

sc = pyspark.SparkContext("local", "WordLetterCount", pyFiles=[])
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "")

text_file = sc.textFile("s3a://group8.foo/asdf")

results = (
    text_file.flatMap(lambda line: re.findall(r"\w+", line))
    .map(lambda word: (word, 1))
    .reduceByKey(lambda x, y: x + y)
    .sortBy(lambda x: x[1], False)
)

results.saveAsTextFile("output.txt")
