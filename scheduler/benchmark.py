from kubernetes import config, client
from scheduler import schedulelib as scheduler
import subprocess
import time
import sys
import os

from common import spark

APPS = [scheduler.App("spark", "driver"), scheduler.App("custom", "")]
EVENT_LOOP_INTERVAL = 1


def run_benchmark(spark_input_url, custom_input_url, spark_nodes, custom_nodes, env):
    config.load_kube_config()
    kube = client.CoreV1Api()

    with scheduler.Scheduler(kube, APPS) as sc:
        assert len(sc.deallocated) >= spark_nodes + custom_nodes
        sc.allocate(APPS[0], spark_nodes)
        sc.allocate(APPS[1], custom_nodes)

        # Setup envs for both
        spark_env = env
        custom_env = env.copy()
        spark_env["APP_NAME"] = APPS[0].name
        custom_env["APP_NAME"] = APPS[1].name

        # Setup commands for both
        spark_cmd = spark.spark_command(spark_input_url, spark_env, schedulable=True)
        custom_cmd = [
            "python",
            os.path.join(
                os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                "wlc-custom/master.py",
            ),
            custom_input_url,
        ]

        # Initalise termination condition and end time variable
        spark_finished = False
        custom_finished = False
        spark_end = None
        custom_end = None

        # Start the processes and appropriate timers
        spark_p = subprocess.Popen(spark_cmd)
        spark_start = time.monotonic()
        custom_p = subprocess.Popen(custom_cmd, env=custom_env)
        custom_start = time.monotonic()

        # Start the event loop
        while not (spark_finished and custom_finished):
            if not spark_finished:
                spark_poll = spark_p.poll()
                if spark_poll is not None:  # TODO: Check for fail status
                    spark_end = time.monotonic()
                    spark_finished = True
            if not custom_finished:
                custom_poll = custom_p.poll()
                if custom_poll is not None:  # TODO: Check for fail status
                    custom_end = time.monotonic()
                    custom_finished = True
            time.sleep(EVENT_LOOP_INTERVAL)

        return (spark_end - spark_start, custom_end - custom_start)


if __name__ == "__main__":
    if len(sys.argv) != 5:
        raise RuntimeError(
            "Usage: benchmark.py <spark-input-url> <custom-input-url> <spark-nodes> <custom-nodes>"
        )

    spark_input_url = sys.argv[1]
    custom_input_url = sys.argv[2]
    spark_nodes = int(sys.argv[3])
    custom_nodes = int(sys.argv[4])

    run_benchmark(
        spark_input_url,
        custom_input_url,
        spark_nodes,
        custom_nodes,
        env=os.environ.copy(),
    )
