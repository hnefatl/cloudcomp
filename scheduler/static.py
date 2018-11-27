from kubernetes import config, client
import scheduler
import sys

APPS = [scheduler.App("spark", "driver"), scheduler.App("custom", "")]


def main():
    if len(sys.argv) != 3:
        raise RuntimeError("Usage: static <spark-nodes> <custom-nodes>")

    spark_nodes = int(sys.argv[1])
    custom_nodes = int(sys.argv[2])

    config.load_kube_config()
    kube = client.CoreV1Api()

    sc = scheduler.Scheduler(kube, APPS)
    assert len(sc.deallocated) >= spark_nodes + custom_nodes
    sc.allocate(APPS[0], spark_nodes)
    sc.allocate(APPS[1], custom_nodes)


if __name__ == "__main__":
    main()
