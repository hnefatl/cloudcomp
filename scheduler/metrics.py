import kubernetes
import subprocess
import json
import yaml
import urllib
import re
import time


# Replace with kubectl top node


# Deploy the metrics server pods onto the cluster
def deploy_metrics():
    # We need to spin up metrics-server pods
    url = "https://raw.githubusercontent.com/kubernetes-incubator/metrics-server/master/deploy/1.8+"
    # Process creation arguments - we append a url to the end
    args = ["kubectl", "create", "-f"]
    # Most of the config files we can use directly
    files = [
        "aggregated-metrics-reader.yaml",
        "auth-delegator.yaml",
        "auth-reader.yaml",
        "metrics-apiservice.yaml",
        "metrics-server-service.yaml",
        "resource-reader.yaml",
    ]
    for file in files:
        subprocess.check_call(args + [f"{url}/{file}"])

    # Edit the main deployment file to allow insecure traffic
    file = "metrics-server-deployment.yaml"
    with urllib.request.urlopen(f"{url}/{file}") as response:
        docs = list(yaml.load_all(response))
    for doc in docs:
        try:
            for container in doc["spec"]["template"]["spec"]["containers"]:
                if container["name"] == "metrics-server":
                    container["args"] = [
                        "--kubelet-insecure-tls",
                        "--metric-resolution=5s",
                    ]
        except KeyError:
            pass

    # Pipe the altered yaml into kubectl create
    with subprocess.Popen(args + ["-"], stdin=subprocess.PIPE) as p:
        yaml.dump_all(docs, stream=p.stdin, encoding="utf-8")


def undeploy_metrics():
    url = "https://raw.githubusercontent.com/kubernetes-incubator/metrics-server/master/deploy/1.8+"
    files = [
        "aggregated-metrics-reader.yaml",
        "auth-delegator.yaml",
        "auth-reader.yaml",
        "metrics-apiservice.yaml",
        "metrics-server-service.yaml",
        "metrics-server-deployment.yaml",
        "resource-reader.yaml",
    ]
    for file in files:
        subprocess.check_call(["kubectl", "delete", "-f", f"{url}/{file}"])


def get_api_client():
    kubernetes.config.load_kube_config()
    return kubernetes.client.ApiClient()


# Returns a dictionary mapping pod names to dictionaries containing the window in seconds of the metrics, the
# timestamp of the measurement the cpu
# usage percentage (0 being 0%, 1 being 100%), and the memory usage in KiB.
def get_node_metrics(api_client):
    response, code, *_ = api_client.call_api(
        "/apis/metrics.k8s.io/v1beta1/nodes", "GET", _preload_content=False
    )
    contents = response.read().decode()
    return json.loads(contents)
    # if code != 200:
    #    raise RuntimeError(f"Got error code {code} from server with body {contents}")
    # metrics = {}
    # for usage_item in json.loads(contents)["items"]:
    #    try:
    #        if usage_item["metadata"]["namespace"] == "kube-system":
    #            continue
    #        containers = usage_item["containers"]
    #        cpu = sum(
    #            convert_cpu(container["usage"]["cpu"]) for container in containers
    #        )
    #        mem = sum(
    #            convert_mem(container["usage"]["memory"]) for container in containers
    #        )
    #        metrics[usage_item["metadata"]["name"]] = {
    #            "window": int(usage_item["window"][:-1]),
    #            "cpu": cpu,
    #            "mem": mem,
    #        }
    #    except KeyError:
    #        pass  # Malformed response, ignore
    # return metrics


def convert_cpu(cpu):
    if cpu.endswith("n"):  # Nanoseconds...
        return int(cpu[:-1]) / 10 ** 9
    else:
        return int(cpu)


def convert_mem(mem_usage):
    if mem_usage.endswith("Ki"):
        return int(mem_usage[:-2])
    elif mem_usage.endswith("Mi"):
        return int(mem_usage[:-2]) * 1024
    elif mem_usage.endswith("Gi"):
        return int(mem_usage[:-2]) * 1024 * 1024
    elif mem_usage.endswith("K"):
        return int(float(mem_usage[:-1]) * 1.024)
    elif mem_usage.endswith("M"):
        return int(float(mem_usage[:-1]) * 1.000_024)
    elif mem_usage.endswith("G"):
        return int(float(mem_usage[:-1]) * 1.000_000_024)
    else:
        return int(mem_usage)


def watch_pod_metrics(api_client):
    can_stop = False
    metrics = {}
    i = 0
    while not can_stop or len(metrics) > 0:
        time.sleep(1)
        i += 1
        metrics = get_pod_metrics(api_client)
        if len(metrics) > 0:
            can_stop = True
        print(f"{i}: {sorted(metrics.items())}")


def test():
    i = 0
    while True:
        i += 1
        print(i)
        subprocess.check_call(["kubectl", "top", "nodes"])
        time.sleep(1)
