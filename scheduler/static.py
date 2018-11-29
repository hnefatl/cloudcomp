from kubernetes import config, client
from scheduler import schedulelib as scheduler
import sys

APPS = [scheduler.App("spark", "driver"), scheduler.App("custom", "master")]


def allocation(spark_input_size, custom_input_size, total_nodes):
    return (total_nodes // 2, total_nodes // 2)


def static(spark_url, spark_input_size, custom_url, custom_input_size, total_nodes):
    config.load_kube_config()
    kube = client.CoreV1Api()

    with scheduler.Scheduler(kube, APPS) as sc:
        spark_nodes, custom_nodes = allocation(
            spark_input_size, custom_input_size, total_nodes
        )
        sc.allocate(APPS[0], spark_nodes)
        sc.allocate(APPS[1], custom_nodes)
        # Start spark and custom


if __name__ == "__main__":
    if len(sys.argv) != 6:
        raise RuntimeError(
            "Usage: static <spark-url> <spark-input-size> <custom-url> <custom-input_size> <total nodes>"
        )

    spark_url = sys.argv[1]
    spark_input_size = int(sys.argv[2])
    custom_url = sys.argv[3]
    custom_input_size = int(sys.argv[4])
    total_nodes = int(sys.argv[5])
    static(spark_url, spark_input_size, custom_url, custom_input_size, total_nodes)
