from kubernetes import config, client
from scheduler import schedulelib as scheduler
import sys

APPS = [scheduler.App("spark", "driver"), scheduler.App("custom", "")]


def allocation(spark_input_size, custom_input_size, total_nodes):
    return (total_nodes // 2, total_nodes // 2)


def static(spark_url, spark_input_size, custom_url, custom_input_size):

    config.load_kube_config()
    kube = client.CoreV1Api()

    with scheduler.Scheduler(kube, APPS) as sc:
        (spark_nodes, custom_nodes) = allocation(spark_input_size, custom_input_size)
        sc.allocate(APPS[0], spark_nodes)
        sc.allocate(APPS[1], custom_nodes)
        # Start spark and custom


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise RuntimeError(
            "Usage: static <spark-url> <spark-input-size> <custom-url> <custom-input_size>"
        )

    spark_url = sys.argv[1]
    spark_input_size = int(sys.argv[2])
    custom_url = sys.argv[3]
    custom_input_size = int(sys.argv[4])
    static(spark_url, spark_input_size, custom_url, custom_input_size)
