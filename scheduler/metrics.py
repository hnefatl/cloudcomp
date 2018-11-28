import kubernetes
import boto3
import collections
import subprocess
import json
import yaml
import urllib
import re
import time


def get_api_client():
    kubernetes.config.load_kube_config()
    return kubernetes.client.CoreV1Api()


# Returns a dictionary mapping each node's app label to a dictionary of cpu/rss/net usage summed over
# all the pods with that app label. Ignores pods without an app label.
# CPU and network metrics are cumulative: the total usage of that resource since the pod was created.
# RSS metric is instantantaneous: the current size.
# CPU is given in fractional seconds of usage, rss and net are given in bytes.
def get_metrics(region, client):
    ec2 = boto3.resource("ec2", region_name=region)
    instances = list(ec2.instances.all())

    kube_nodes = {
        node.metadata.name
        for node in client.list_node(label_selector="kubernetes.io/role=node").items
    }

    pod_label_map = {
        pod.metadata.uid: pod.metadata.labels["app"]
        for pod in client.list_pod_for_all_namespaces(watch=False).items
        if "app" in pod.metadata.labels
    }

    instance_ips = []
    for instance in instances:
        if instance.private_dns_name in kube_nodes:
            instance_ips.append(instance.public_dns_name)

    output = collections.defaultdict(lambda: collections.defaultdict(int))
    for instance_ip in instance_ips:
        response = urllib.request.urlopen(f"http://{instance_ip}:10255/stats/summary")
        if response.code != 200:
            continue  # Just skip failed responses
        stats = json.loads(response.read().decode())
        for pod in stats["pods"]:
            uid = pod["podRef"]["uid"]
            app_label = pod_label_map.get(uid, None)
            if app_label is None:
                continue  # Skip pods that we didn't know existed

            cpu = pod["cpu"]["usageCoreNanoSeconds"]
            rss = pod["memory"]["rssBytes"]
            rx = pod["network"]["rxBytes"]
            tx = pod["network"]["txBytes"]

            output[app_label]["cpu"] += cpu / 10 ^ 9
            output[app_label]["memory"] += rss
            output[app_label]["received"] += rx
            output[app_label]["sent"] += tx

    return dict(output)