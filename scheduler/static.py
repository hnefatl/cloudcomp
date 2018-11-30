from kubernetes import config, client
from scheduler import schedulelib as scheduler
import sys
import numpy as np
from scipy.optimize import curve_fit
import json
import os.path
from math import ceil, floor
import subprocess
import time
from common import spark

APPS = [scheduler.App("spark", "driver"), scheduler.App("custom", "master")]
EVENT_LOOP_INTERVAL = 1


def allocation(spark_input_size, custom_input_size, total_nodes):
    assert total_nodes >= 4
    data_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data.json")
    f_spark = fit_data(data_file, "spark")

    f_custom = fit_data(data_file, "custom")

    min_spark = ceil(total_nodes / 2)
    min_custom = floor(total_nodes / 2)
    min_val = max(
        f_spark(min_spark, spark_input_size), f_custom(min_custom, custom_input_size)
    )
    for spark_nodes in range(2, total_nodes - 1):
        custom_nodes = total_nodes - spark_nodes
        val = max(
            f_spark(spark_nodes, spark_input_size),
            f_custom(custom_nodes, custom_input_size),
        )
        if val < min_val:
            min_val = val
            min_spark = spark_nodes
            min_custom = custom_nodes
    return (min_spark, min_custom)


# Returns execution_time = f(num_nodes, input_size)
# Model used is f(n, i) = mi/n
def fit_data(data_file, app_name):
    with open(data_file) as f:
        data = json.loads(f.read())[app_name]
        x_data = [[], []]
        y_data = []
        for num_nodes, v in data.items():
            for input_size, execution_time in v.items():
                x_data[0].append(int(num_nodes))
                x_data[1].append(int(input_size))
                y_data.append(int(execution_time))

        x_data = np.array(x_data)
        y_data = np.array(y_data)
        params, covariance = curve_fit(lambda x, m: (m * x[1]) / x[0], x_data, y_data)
        return lambda n, i: (params[0] * i) / n


def static(
    spark_url, spark_input_size, custom_url, custom_input_size, total_nodes, env
):
    config.load_kube_config()
    kube = client.CoreV1Api()

    with scheduler.Scheduler(kube, APPS) as sc:
        spark_nodes, custom_nodes = allocation(
            spark_input_size, custom_input_size, total_nodes
        )
        sc.allocate(APPS[0], spark_nodes)
        sc.allocate(APPS[1], custom_nodes)

        # Setup envs for both
        spark_env = env
        custom_env = env.copy()
        spark_env["APP_NAME"] = APPS[0].name
        custom_env["APP_NAME"] = APPS[1].name

        # Setup commands for both
        spark_cmd = spark.spark_command(
            spark_url, "eu-west-2", spark_env, schedulable=True
        )
        custom_cmd = [
            "python",
            os.path.join(
                os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                "wlc-custom/deploy.py",
            ),
            custom_url,
        ]

        # Initialise termination condition and end time variable
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
        print(f"Spark: {spark_end - spark_start}")
        print(f"Custom: {custom_end - custom_start}")


if __name__ == "__main__":
    if len(sys.argv) != 6:
        raise RuntimeError(
            "Usage: static <spark-url> <spark-input-size> <custom-url> <custom-input_size> <total nodes>"
        )

    spark_url = sys.argv[1]
    spark_input_size = int(sys.argv[2])
    custom_url = sys.argv[3]
    custom_input_size = int(sys.argv[4])
    total_nodes = int(sys.argv[5])
    static(
        spark_url,
        spark_input_size,
        custom_url,
        custom_input_size,
        total_nodes,
        os.environ(),
    )
