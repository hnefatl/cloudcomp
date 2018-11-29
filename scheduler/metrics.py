import kubernetes
import boto3
import collections
import subprocess
import json
import yaml
import urllib
import re
import time


def get_client():
    kubernetes.config.load_kube_config()
    return kubernetes.client.CoreV1Api()


class Metrics:
    def __init__(self):
        self.cpu = 0
        self.rss = 0
        self.rx = 0
        self.tx = 0

    def add(self, b):
        self.cpu += b.cpu
        self.rss += b.rss
        self.rx += b.rx
        self.tx += b.tx

    def subtract(self, b):
        self.cpu -= b.cpu
        self.rss -= b.rss
        self.rx -= b.rx
        self.tx -= b.tx

    def __str__(self):
        return f"Metrics(cpu={self.cpu}, rss={self.rss}, rx={self.rx}, tx={self.tx})"

    def __repr__(self):
        return self.__str__()


class MetricsCollector:
    def __init__(self, client, region):
        self._client = client
        self._region = region
        ec2 = boto3.resource("ec2", region_name=self._region)
        self._instances = list(ec2.instances.all())
        # Maps app to current metrics for that node
        self._app_metrics = collections.defaultdict(Metrics)
        # Stores metrics per pod uid, used for computing the total historic metrics for an app
        # after it deletes pods
        self._pod_metrics = collections.defaultdict(Metrics)

        # All kubernetes node names
        kube_nodes = {
            node.metadata.name
            for node in self._client.list_node(
                label_selector="kubernetes.io/role=node"
            ).items
        }
        # Public IPs of all the EC2 instances
        self._instance_ips = []
        for instance in self._instances:
            if instance.private_dns_name in kube_nodes:
                self._instance_ips.append(instance.public_dns_name)

    def update_metrics(self):
        # Maps the UID of all pods with an "app" label to the value of that label
        pod_label_map = {
            pod.metadata.uid: pod.metadata.labels["app"]
            for pod in self._client.list_pod_for_all_namespaces(
                watch=False, field_selector="status.phase=Running", label_selector="app"
            ).items
        }

        for instance_ip in self._instance_ips:
            # Pull the stats from the kubelet API
            response = urllib.request.urlopen(
                f"http://{instance_ip}:10255/stats/summary"
            )
            if response.code != 200:
                continue  # Just skip failed responses
            stats = json.loads(response.read().decode())
            for pod in stats["pods"]:
                uid = pod["podRef"]["uid"]
                app_label = pod_label_map.get(uid, None)
                if app_label is None:
                    continue  # Skip pods that we didn't know existed

                # If we're missing output for a field, default it to None
                cpu = pod["cpu"].get("usageCoreNanoSeconds", None)
                rss = pod["memory"].get("rssBytes", None)
                rx = pod["network"].get("rxBytes", None)
                tx = pod["network"].get("txBytes", None)

                # If we've seen this pod before, decrease the running sum by the last
                # value we saw before increasing it with the new value
                if uid in self._pod_metrics:
                    self._app_metrics[app_label].subtract(self._pod_metrics[uid])

                # Update our historic record of what this pod's used
                if cpu is not None:
                    self._pod_metrics[uid].cpu = cpu / 10 ** 9
                if rss is not None:
                    self._pod_metrics[uid].rss = rss
                if rx is not None:
                    self._pod_metrics[uid].rx = rx
                if tx is not None:
                    self._pod_metrics[uid].tx = tx

                # Update our running total of what this app's used
                self._app_metrics[app_label].add(self._pod_metrics[uid])

    # Returns a dictionary mapping each node's app label to a dictionary of cpu/rss/net usage summed over
    # all the pods with that app label. Ignores pods without an app label.
    # CPU and network metrics are cumulative: the total usage of that resource since the pod was created.
    # RSS metric is instantantaneous: the current size.
    # CPU is given in fractional seconds of usage, rss and net are given in bytes.
    def get_metrics(self):
        return dict(self._app_metrics)

    def get_app_metrics(self, app_name):
        return self._app_metrics[app_name]