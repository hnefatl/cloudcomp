from kubernetes import config, client
from scheduler import schedulelib as scheduler
import subprocess
import time
import sys
import os

from common import spark

APPS = [scheduler.App("spark", "driver"), scheduler.App("custom", "")]


def benchmark(f):
    def benchmarked_f(*args):
        start = time.monotonic()
        f(*args)
        end = time.monotonic()
        return end - start

    return benchmarked_f


@benchmark
def run_spark(input_url, env):
    env["APP_NAME"] = APPS[0].name
    cmd = spark.spark_command(input_url, env, schedulable=True)
    subprocess.check_call(cmd)


@benchmark
def run_custom(input_url, env):
    env["APP_NAME"] = APPS[1].name
    cmd = [
        "python",
        os.path.join(
            os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
            "wlc-custom/master.py",
        ),
        input_url,
    ]
    print(cmd)
    subprocess.check_call(cmd, env=env)


def run_benchmark(spark_input_url, custom_input_url, spark_nodes, custom_nodes, env):
    config.load_kube_config()
    kube = client.CoreV1Api()

    with scheduler.Scheduler(kube, APPS) as sc:
        assert len(sc.deallocated) >= spark_nodes + custom_nodes
        sc.allocate(APPS[0], spark_nodes)
        sc.allocate(APPS[1], custom_nodes)
        spark_time = run_spark(spark_input_url, env)
        custom_time = run_custom(custom_input_url, env)
        return (spark_time, custom_time)


if __name__ == "__main__":
    if len(sys.argv) != 5:
        raise RuntimeError(
            "Usage: benchmark.py <spark-input-url> <custom-input-url> <spark-nodes> <custom-nodes>"
        )

    spark_input_url = sys.argv[1]
    custom_input_url = sys.argv[2]
    spark_nodes = int(sys.argv[3])
    custom_nodes = int(sys.argv[4])

    run_benchmark(
        spark_input_url,
        custom_input_url,
        spark_nodes,
        custom_nodes,
        env=os.environ.copy(),
    )
