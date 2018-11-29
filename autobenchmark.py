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
            proc.stdin.writelines(
                "\n".join(
                    [
                        "12",  # Enter existing cluster
                        cluster,  # cluster url
                        "7",  # Run benchmark
                        "3",  # 3 trials
                        spark_file_size,
                        custom_file_size,
                        num_spark,
                        num_custom,
                        "0",
                    ]
                )
            )
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
