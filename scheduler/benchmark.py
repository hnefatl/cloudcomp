from kubernetes import config, client
import scheduler.schedulelib as scheduler
import time
import sys

APPS = [scheduler.App("spark", "driver"), scheduler.App("custom", "")]


def benchmark(f):
    def benchmarked_f():
        start = time.monotonic()
        f()
        end = time.monotonic()
        return end - start

    return benchmarked_f


@benchmark
def run_spark():
    pass


@benchmark
def run_custom():
    pass


def run_benchmark(spark_input_url, custom_input_url, spark_nodes, custom_nodes):
    config.load_kube_config()
    kube = client.CoreV1Api()

    with scheduler.Scheduler(kube, APPS) as sc:
        assert len(sc.deallocated) >= spark_nodes + custom_nodes
        sc.allocate(APPS[0], spark_nodes)
        sc.allocate(APPS[1], custom_nodes)
        # Start spark and custom apps


if __name__ == "__main__":
    if len(sys.argv) != 5:
        raise RuntimeError(
            "Usage: benchmark.py <spark-input-url> <custom-input-url> <spark-nodes> <custom-nodes>"
        )

    spark_input_url = sys.argv[1]
    custom_input_url = sys.argv[2]
    spark_nodes = int(sys.argv[3])
    custom_nodes = int(sys.argv[4])

    run_benchmark(spark_input_url, custom_input_url, spark_nodes, custom_nodes)
