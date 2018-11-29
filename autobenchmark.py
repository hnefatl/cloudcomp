#!/usr/bin/env python

import itertools
import subprocess
import sys
import os
import shutil
import traceback


def runcli(
    cluster, output_dir, spark_file_size, custom_file_size, num_spark, num_custom
):
    with open(
        f"{output_dir}/{spark_file_size}-{custom_file_size}-{num_spark}-{num_custom}",
        "w+",
    ) as f:
        try:
            proc = subprocess.Popen(
                ["python", "cli.py"], stdin=subprocess.PIPE, stdout=f, stderr=f
            )
            inputs = [
                "12",  # Enter existing cluster
                cluster,  # cluster url
                "7",  # Run benchmark
                "3",  # 3 trials
                str(spark_file_size),
                str(custom_file_size),
                str(num_spark),
                str(num_custom),
                "0",
            ]
            proc.stdin.write("\n".join(inputs).encode())
            proc.wait()
        except Exception:
            traceback.print_exc(file=f)


def main():
    if len(sys.argv) != 3:
        print("Usage: autobenchmark <cluster s3 url> <output directory>")
        return

    cluster_url = sys.argv[1]
    output_dir = sys.argv[2]

    shutil.rmtree(output_dir, ignore_errors=True)
    os.makedirs(output_dir, exist_ok=True)

    sizes = [200, 400, 500]
    balances = [(2, 8), (5, 5), (8, 2)]

    for spark_size, custom_size, (num_spark, num_custom) in itertools.product(
        sizes, sizes, balances
    ):
        print(f"Benchmarking {spark_size} {custom_size} {num_spark} {num_custom}")
        runcli(cluster_url, output_dir, spark_size, custom_size, num_spark, num_custom)


if __name__ == "__main__":
    main()
