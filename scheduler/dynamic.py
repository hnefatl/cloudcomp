#! /usr/bin/env python3
from collections import namedtuple
import collections
import functools
import time
import os
import subprocess

from scheduler.schedulelib import Scheduler, App
import scheduler.metrics as metrics
import common.spark as spark


# Bounds check
# Div by zero
APPS = [App("spark", "driver"), App("custom", "master")]
DECISION_PERIOD = 30


def app_running(client, app):
    podList = client.list_namespaced_pod(
        "default",
        field_selector="status.phase!=Succeeded,status.phase!=Failed,status.phase!=Unknown",
    )
    return any(app.is_master(pod.metadata.name) for pod in podList.items)


class DynamicState:
    def __init__(self, scheduler):
        self._print_stats = True
        self._scheduler = scheduler
        # How many nodes we can add/remove in one scheduling decision
        self._node_deltas = [-2, -1, 0, 1, 2]
        # We define "cpu utilisation" to be the local gradient of the cumulative
        # CPU divided by the number of instances.
        # We store sampled values of this frequently inbetween scheduling decisions
        # and base decisions off the gradient of the points (the change in cpu
        # utilisation since the last scheduling decision, which we assume is
        # attributed purely to our scheduler).
        self._previous_cpu_util = {app.name: 0 for app in APPS}
        self._previous_cumulative_cpu = {app.name: 0 for app in APPS}
        self._last_decision_time = time.monotonic()

    # Returns the number of nodes to transfer from app1 -> app2 (or negative for app2 -> app1)
    def make_scheduling_decision(self, app_metrics):
        current_decision_time = time.monotonic()
        time_change = current_decision_time - self._last_decision_time
        current_utilisation = {app.name: 0 for app in APPS}
        predicted_utilisation = {
            delta: {app.name: 0 for app in APPS} for delta in self._node_deltas
        }

        # Compute the predicted utilisation area for all apps and all deltas
        for app in APPS:
            # Find the gradient of cumulative cpu since the last decision
            cumulative_cpu = app_metrics[app.name].cpu
            cumulative_cpu_gradient = (
                cumulative_cpu - self._previous_cumulative_cpu[app.name]
            ) / self._last_decision_time

            # Find the current cpu utilisation
            num_instances = len(self._scheduler.allocated[app.name])
            cpu_util = cumulative_cpu_gradient / num_instances
            current_utilisation[app.name] = cpu_util

            # Project each possible instance delta into the future and compute
            # the predicted cumulative cpu area we cover by performing that delta
            for delta in self._node_deltas:
                # Don't allow less than 2 nodes per app
                if num_instances + delta < 2:
                    continue
                # We scale the expected gradient by the ratio of new nodes to old nodes
                # to take into account the cost/benefit of distributing over more/fewer nodes
                instance_scale = num_instances / (num_instances + delta)
                predicted_utilisation[delta][app.name] = (
                    cpu_util * instance_scale * time_change
                )

            self._previous_cumulative_cpu[app.name] = cumulative_cpu
            self._previous_cpu_util[app.name] = cpu_util

        def avg_predicted_area(delta):
            def area(name, delta):
                current = current_utilisation[name]
                predicted = predicted_utilisation[delta][name]
                # Trapezium rule
                return (current + predicted) / 2

            # Average of the two areas
            # We negate the delta of the second app as we're either taking away/adding to
            # it the inverse of what we're adding to/taking away from the first app.
            try:
                return (
                    (area(APPS[0].name, delta) + area(APPS[1].name, -delta))
                    * time_change
                    / 2
                )
            except KeyError:  # Invalid delta, return 0 area so we don't pick
                return 0

        # argmax of delta across predicted areas
        if self._print_stats:
            for app in APPS:
                print(f"{app.name}'s CPU: {app_metrics[app.name].cpu}")
            for delta in self._node_deltas:
                print(f"Delta: {delta}")
                print(f"  Area: {avg_predicted_area(delta)}")
                for app in APPS:
                    print(f"  App: {app.name}")
                    print(f"    Current: {current_utilisation[app.name]}")
                    print(f"    Predicted: {predicted_utilisation[delta][app.name]}")
        best_delta = max(self._node_deltas, key=avg_predicted_area)
        self._last_decision_time = time.monotonic()
        return best_delta


def schedule(region, spark_input_url, custom_input_url, env):
    kube_client = metrics.get_client()
    metrics_collector = metrics.MetricsCollector(
        kube_client, region, [app.name for app in APPS]
    )

    with Scheduler(kube_client, APPS) as sc:
        state = DynamicState(sc)

        # Initial allocation
        n = len(sc.deallocated)
        spark_nodes = n // 2
        custom_nodes = n - spark_nodes
        sc.allocate(APPS[0], spark_nodes)
        sc.allocate(APPS[1], custom_nodes)

        # Setup envs for both
        spark_env = env
        custom_env = env.copy()
        spark_env["APP_NAME"] = APPS[0].name
        custom_env["APP_NAME"] = APPS[1].name

        # Setup commands for both
        spark_cmd = spark.spark_command(
            spark_input_url, "eu-west-2", spark_env, schedulable=True
        )
        custom_cmd = [
            "python",
            os.path.join(
                os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                "wlc-custom/deploy.py",
            ),
            custom_input_url,
        ]

        spark_process = subprocess.Popen(spark_cmd, env=spark_env)
        custom_process = subprocess.Popen(custom_cmd, env=custom_env)

        # Wait for pods to start
        print("Waiting for app's masters to start")
        app1_running = False
        app2_running = False
        while not (app1_running and app2_running):
            time.sleep(1)
            app1_running = app_running(kube_client, APPS[0])
            app2_running = app_running(kube_client, APPS[1])

        last_decision = time.monotonic()
        while app1_running or app2_running:
            time.sleep(1)
            # Check if either app has finished
            # If an app's finished, transfer all its allocated nodes to the other app
            if app1_running:
                app1_running = app_running(kube_client, APPS[0])
                # The application just finished
                if not app1_running:
                    sc.transfer(APPS[0], APPS[1], len(sc.allocated[APPS[0].name]))
            if app2_running:
                app2_running = app_running(kube_client, APPS[1])
                # The application just finished
                if not app2_running:
                    sc.transfer(APPS[1], APPS[0], len(sc.allocated[APPS[1].name]))

            print(f"{APPS[0].name}: {app1_running}, {APPS[1].name}: {app2_running}")
            # If either app has finished, we don't need to update resources so
            # just keep looping.
            if not app1_running or not app2_running:
                continue

            # Get new metrics
            metrics_collector.update_metrics()

            if last_decision + DECISION_PERIOD < time.monotonic():
                last_decision = time.monotonic()

                app_metrics = metrics_collector.get_metrics()
                # Transfer nodes if necessary
                n = state.make_scheduling_decision(app_metrics)
                if n > 0:
                    print(
                        f"Scheduler allocates {n} nodes from {APPS[0].name} to {APPS[1].name}"
                    )
                    sc.transfer(APPS[0], APPS[1], n)
                elif n < 0:
                    print(
                        f"Scheduler allocates {-n} nodes from {APPS[1].name} to {APPS[0].name}"
                    )
                    sc.transfer(APPS[1], APPS[0], -n)
                else:
                    print(f"Scheduler makes no changes")

        code = spark_process.wait()
        if code != 0:
            raise RuntimeError(f"Spark exited with error code {code}")
        code = custom_process.wait()
        if code != 0:
            raise RuntimeError(f"Custom exited with error code {code}")
