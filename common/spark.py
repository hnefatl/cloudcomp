#! /usr/bin/env python3
import subprocess
import os
import sys

SPARK_BASE = [
    "spark-submit",
    # General config
    "--deploy-mode",
    "cluster",
    "--name",
    "wordlettercount",
    "--conf",
    "spark.kubernetes.pyspark.pythonVersion=3",
    "--conf",
    "spark.kubernetes.container.image=docker.io/clgroup8/wordlettercount:latest",
    "--conf",
    "spark.kubernetes.container.image.pullPolicy=Always",
    "--conf",
    "spark.driver.cores=0.6",
    "--conf",
    "spark.kubernetes.executor.request.cores=0.6",
]

SPARK_FILE = "local:///usr/spark-2.4.0/work-dir/wordlettercount.py"


def get_spark_base(env):
    return SPARK_BASE + [
        "--master",
        f"k8s://{env['KUBERNETES_MASTER']}",
        "--conf",
        f"spark.executor.instances={env['NUMBER_OF_NODES']}",
    ]


def enable_scheduling_command(env):
    return [
        "--conf",
        f"spark.kubernetes.driver.label.app={env['APP_NAME']}",
        "--conf",
        f"spark.kubernetes.executor.label.app={env['APP_NAME']}",
        "--conf",
        f"spark.kubernetes.node.selector.app={env['APP_NAME']}",
    ]


def get_wlc_cmd(input_url, env):
    return [
        SPARK_FILE,
        env["AWS_ACCESS_KEY_ID"],
        env["AWS_SECRET_ACCESS_KEY"],
        env["RDS_HOST"],
        env["RDS_PORT"],
        env["RDS_USERNAME"],
        env["RDS_PASSWORD"],
        env["RDS_DB_NAME"],
        input_url,
    ]


def spark_command(input_url, env, schedulable=False):
    cmd = get_spark_base(env)
    if schedulable:
        cmd.extend(enable_scheduling_command(env))
    cmd.extend(get_wlc_cmd(input_url, env))
    return cmd


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise RuntimeError("usage: spark.py <input_url>")
    subprocess.check_call(spark_command(sys.argv[1], os.environ))
