#! /usr/bin/env python3
from collections import namedtuple
from scheduler.schedulelib import Scheduler, App
import functools

import metrics


APPS = [App("spark", "driver"), App("custom", "")]


def app_running(client, app):
    podList = client.list_namespaced_pod(
        "default", field_selector="status.phase!=Succeeded"
    )
    return any(app.is_master(pod.metadata.name) for pod in podList.items)


# Returns the number of nodes to transfer from app1 -> app2 (or negative for app2 -> app1)
def nodes_to_transfer(sc: Scheduler, app1_metrics, app2_metrics) -> int:
    return 0


def schedule(region):
    kube_client = metrics.get_client()
    metrics_collector = metrics.MetricsCollector(kube_client, region)

    with Scheduler(kube_client, APPS) as sc:
        # Initial allocation
        n = len(sc.deallocated)
        spark_nodes = n // 2
        custom_nodes = n - spark_nodes
        sc.allocate(APPS[0], spark_nodes)
        sc.allocate(APPS[1], custom_nodes)

        # TODO: Start apps

        app1_running = True
        app2_running = True
        while app1_running or app2_running:
            # Check if either app has finished
            if app1_running:
                app1_running = app_running(kube_client, APPS[0])
            if app2_running:
                app2_running = app_running(kube_client, APPS[1])
            # If either app has finished, we don't need to update resources
            if not app1_running or not app2_running:
                continue

            # Get new metrics
            app1_metrics = metrics_collector.get_app_metrics(APPS[0].name)
            app2_metrics = metrics_collector.get_app_metrics(APPS[1].name)

            # Transfer nodes if necessary
            n = nodes_to_transfer(sc, app1_metrics, app2_metrics)
            if n > 0:
                sc.transfer(APPS[0], APPS[1], n)
            elif n < 0:
                sc.transfer(APPS[1], APPS[0], n)


if __name__ == "__main__":
    # schedule()
    pass
