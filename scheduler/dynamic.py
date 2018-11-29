#! /usr/bin/env python3
from collections import namedtuple
from kubernetes import config, client
from scheduler.schedulelib import Scheduler, App

# Metrics represents the potential metrics that the scheduler can base decisions on
# name - is a string containing the name of the app exporting the Metrics
# cpu - is a float (0 < cpu < 1) representing CPU utilisation
# memory - is a float (0 < memory < 1) representing memory utilisation
# outstanding - is the number of outstanding pods (only really relevant in the custom app)
# finished - is a boolean value representing the status of the job
Metrics = namedtuple("Metrics", ["name", "cpu", "memory", "outstanding", "finished"])


class SparkCollector:
    _num = 0

    def __init__(self):
        self.app = App(f"spark-{SparkCollector._num}", "driver")
        SparkCollector._num += 1
        pass

    def get_metrics(self) -> Metrics:
        pass


class CustomCollector:
    _num = 0

    def __init__(self):
        self.app = App(f"custom-{CustomCollector._num}", "master")
        CustomCollector._num += 1
        pass

    def get_metrics(self) -> Metrics:
        pass


# Returns the number of nodes to transfer from app1 -> app2 (or negative for app2 -> app1)
def nodes_to_transfer(
    sc: Scheduler, app1_metric: Metrics, app2_metrics: Metrics
) -> int:
    return 0


def terminate(app1_metrics: Metrics, app2_metrics: Metrics) -> bool:
    return app1_metrics.finished and app2_metrics.finished


def get_apps():
    return (SparkCollector(), CustomCollector())


def schedule():
    # STAGES:
    config.load_cluster_config()
    kube = client.CoreV1Api()
    apps = get_apps()
    with Scheduler(kube, apps) as sc:
        n = len(sc.deallocated())
        spark_nodes = n // 2
        custom_nodes = n - spark_nodes
        sc.allocate(apps[0], spark_nodes)
        sc.allocate(apps[1], custom_nodes)
        # TODO: Start apps
        terminate = False
        while not terminate:
            # Potentially parallellize these calls?
            app1_metrics = apps[0].get_metrics()
            app2_metrics = apps[1].get_metrics()
            n = nodes_to_transfer(sc, app1_metrics, app2_metrics)
            if n > 0:
                sc.transfer(apps[0], apps[1], n)
            elif n < 0:
                sc.transfer(apps[1], apps[0], n)
            terminate = terminate(app1_metrics, app2_metrics)


if __name__ == "__main__":
    schedule()
