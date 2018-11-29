#! /usr/bin/env python3
from kubernetes import client, config
import os
import re
import random
import string
import sys
import time

EVENT_LOOP_INTERVAL = 1


def build_env(env):
    new_env = []
    for k, v in env.items():
        if re.fullmatch(r"[-._a-zA-Z][-._a-zA-Z0-9]*", k):
            new_env.append(client.V1EnvVar(name=k, value=v))

    return new_env


def start_master(*args):
    config.load_kube_config()
    kube = client.CoreV1Api()

    app_name = os.environ["APP_NAME"]
    labels = None
    if app_name != "":
        labels = {"app": app_name}

    env = os.environ.copy()
    name = f"master-{env['MASTER_ID']}"

    env = build_env(env)

    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(name=name, labels=labels),
        spec=client.V1PodSpec(
            restart_policy="Never",
            containers=[
                client.V1Container(
                    image="docker.io/clgroup8/master",
                    name=name,
                    env=env,
                    args=args,
                    resources=client.V1ResourceRequirements(
                        requests={"cpu": "400m", "memory": "500Mi"}
                    ),
                )
            ],
            node_selector=labels,
        ),
    )

    kube.create_namespaced_pod("default", pod)
    return name


def wait_for_master(name):
    config.load_kube_config()
    kube = client.CoreV1Api()
    while (
        len(
            kube.list_namespaced_pod(
                "default",
                field_selector=f"metadata.name=={name},status.phase!=Succeeded",
            ).items
        )
        == 1
    ):
        time.sleep(EVENT_LOOP_INTERVAL)


def main():
    if len(sys.argv) < 2:
        raise RuntimeError("usage: deploy.py ARGS")

    config.load_kube_config()
    name = start_master(*sys.argv[1:])
    wait_for_master(name)


if __name__ == "__main__":
    main()
