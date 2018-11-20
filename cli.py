#!/usr/bin/env python3.7

import boto3
import subprocess
import string
import random
import tempfile
import os
import re
import time
import json
import pathlib

import clusterconfig
import rds


class Interface:
    def __init__(self, aws_access_key, aws_secret_key):
        self._config = clusterconfig.ClusterConfig()
        self._aws_access_key = aws_access_key
        self._aws_secret_key = aws_secret_key
        self._db_name = "kc506_rc691_CloudComputingCoursework"
        self._running = True
        self._dry_run = False
        self._cluster_started = False
        self._s3_bucket_path = None

        # Map an input number to an action
        self._action_dict = {
            "0": self.stop,
            "1": self.edit_cluster_definition,
            "11": self.print_cluster_definition,
            "12": self.set_existing_cluster,
            "2": self.start_cluster,
            "21": self.validate_cluster,
            "22": self.deploy_dashboard,
            "23": self.access_dashboard,
            "3": self.view_cluster,
            "31": self.get_admin_password,
            "32": self.get_admin_service_token,
            "4": self.delete_cluster,
            "5": self.run_spark_app,
            "51": self.view_spark_app,
            "52": self.view_spark_app_output,
            "c": lambda: subprocess.check_call("clear"),
        }

    def __enter__(self):
        return self

    def __exit__(self, *_):
        # Don't automatically kill the cluster when we exit: useful for dev
        # if self._cluster_started:
        #    self.delete_cluster()
        if self._s3_bucket_path is not None:
            print(f"Leaving cluster running on {self._s3_bucket_path}")

    def run(self):
        while self._running:
            self.show_menu()
            action = self.get_action()
            try:
                action()
            except Exception as e:
                print(e)

    def show_menu(self):
        print("0: Exit")
        print("1: Define a Kubernetes Cluster")
        print("    11: Review the cluster definition")
        print("    12: Load an existing cluster")
        print("2: Launch the cluster on AWS")
        print("    21: Validate the cluster")
        print("    22: Deploy the Kubernetes web dashboard")
        print("    23: Access the Kubernetes web dashboard")
        print("3: View the cluster")
        print("    31: Get the admin password")
        print("    32: Get the admin service account token")
        print("4: Delete the cluster")
        print("5: Run Spark WordLetterCount App")
        print("    51: View Spark App")
        print("    52: Show Output")

    def get_action(self):
        try:
            selection = input("Please enter your choice: ")
            return self._action_dict[selection]
        except EOFError:
            print()  # Newline puts user's cursor on following line
            return self.stop  # On EOF (ctrl-d) stop graciously
        except Exception:
            return self.invalid_entry

    def stop(self):
        self._running = False

    def invalid_entry(self):
        print("Invalid entry")

    def print_cluster_definition(self):
        print(self._config.json_show())

    def edit_cluster_definition(self):
        # Let the user edit the JSON config
        with tempfile.NamedTemporaryFile(mode="w+", delete=False) as f:
            f.write(self._config.json_show())

        result = subprocess.call("$EDITOR " + f.name, shell=True)
        if result != 0:
            print("Failed to edit file")
            return
        self._config.json_load_path(f.name)

    def start_cluster(self):
        if self._cluster_started:
            print("Cluster already started")
            return

        self._s3_bucket_path = (
            f"s3://{self._config.s3_bucket_prefix}.{self._generate_random_string()}"
        )

        # Create resources
        print(f"Creating s3 bucket {self._s3_bucket_path}")
        self._run_aws(["s3", "mb", self._s3_bucket_path]).check_returncode()
        print(f"Creating cluster: {self._config.cluster_name} in {self._config.zone}")
        self._run_kops(
            [
                "create",
                "cluster",
                self._config.cluster_name,
                "--zones",
                self._config.zone,
                "--authorization",
                "AlwaysAllow",
                "--master-count",
                str(self._config.init_master_count),
                "--master-size",
                self._config.master_type,
                "--node-size",
                self._config.slave_type,
                "--node-count",
                str(self._config.init_slave_count),
                "--yes",
            ]
        ).check_returncode()

        # Run resources
        print("Running cluster")
        self._run_kops(
            ["update", "cluster", self._config.cluster_name, "--yes"]
        ).check_returncode()
        self._cluster_started = True

    def delete_cluster(self):
        if not self._cluster_started:
            print("Cluster not started")
            return

        # Delete resources
        print("Deleting cluster")
        self._run_kops(["delete", "cluster", self._config.cluster_name, "--yes"])
        print("Deleting S3 bucket")
        self._run_aws(["s3", "rb", self._s3_bucket_path, "--force"]).check_returncode()
        self._s3_bucket_path = None
        self._cluster_started = False

    def set_existing_cluster(self):
        self._s3_bucket_path = input(
            "Enter state store url (eg. s3://kubernetes.group8 or kubernetes.group8): "
        )
        if not self._s3_bucket_path.startswith("s3://"):
            self._s3_bucket_path = "s3://" + self._s3_bucket_path
        self._cluster_started = True

    def validate_cluster(self):
        if not self._cluster_started:
            print("Cluster not started")
            return

        self._run_kops(
            ["validate", "cluster", self._config.cluster_name]
        ).check_returncode()

    def view_cluster(self):
        if not self._cluster_started:
            print("Cluster not started")
            return
        self._run_kops(["get", "cluster", self._config.cluster_name]).check_returncode()
        self._run_kops(["get", "ig"]).check_returncode()

    def deploy_dashboard(self):
        print("Creating dashboard")
        self._run_kubectl(
            [
                "create",
                "-f",
                "https://raw.githubusercontent.com/kubernetes/kops/master/addons/kubernetes-dashboard/v1.8.3.yaml",
            ]
        ).check_returncode()

    def access_dashboard(self):
        print("Launching proxy")
        proxy = subprocess.Popen(["kubectl", "proxy"])
        time.sleep(1)

        print(
            "Open http://localhost:8001/api/v1/namespaces/kube-system/services/https:kubernetes-dashboard:/proxy/"
        )

        input("Press enter to kill proxy")
        proxy.kill()
        proxy.wait()
        print("Proxy killed")

    def get_admin_password(self):
        print("Getting Admin Password:\n")
        self._run_kops(
            ["get", "secrets", "--type", "secret", "admin", "-o", "plaintext"]
        ).check_returncode()

    def get_admin_service_token(self):
        print("Getting Admin Service Token:\n")
        try:
            sa_proc = self._run_kubectl(
                ["get", "serviceaccount", "default", "-o", "json"], capture_output=True
            )
            sa_proc.check_returncode()
            sa = json.loads(sa_proc.stdout)
            sa_token_name = sa["secrets"][0]["name"]
            sa_secrets_proc = self._run_kubectl(
                ["get", "secrets", sa_token_name, "-o", "json"], capture_output=True
            )
            sa_secrets_proc.check_returncode()
            sa_secrets = json.loads(sa_secrets_proc.stdout)
            print(sa_secrets["data"]["token"])
        except (KeyError, IndexError, json.decoder.JSONDecodeError):
            print("Unable to retrieve key")
            raise

    def run_spark_app(self):
        input_url = input(
            "Enter s3 url to the input file (eg. s3a://kubernetes.group8/input.txt): "
        )

        print("Resetting database tables")
        rds.initialise_instance(
            host=self._config.rds_host,
            port=self._config.rds_port,
            db_name=self._db_name,
            username=self._config.rds_username,
            password=self._config.rds_password,
        )
        print("Starting spark job")
        # Run the spark job
        subprocess.check_call(
            [
                "spark-submit",
                # General config
                "--master",
                f"k8s://{self._get_master_endpoint()}",
                "--deploy-mode",
                "cluster",
                "--name",
                "wordlettercount",
                "--conf",
                "spark.executor.instances=2",
                "--conf",
                "spark.kubernetes.pyspark.pythonVersion=3",
                "--conf",
                "spark.kubernetes.container.image=docker.io/clgroup8/wordlettercount:latest",
                "--conf",
                "spark.kubernetes.container.image.pullPolicy=Always",
                # Script to run
                "file:///usr/spark-2.4.0/work-dir/wordlettercount.py",
                # Arguments to the script
                self._aws_access_key,
                self._aws_secret_key,
                self._config.rds_host,
                str(self._config.rds_port),
                self._config.rds_username,
                self._config.rds_password,
                self._db_name,
                input_url,
            ]
        )

    def view_spark_app(self):
        directory = os.path.dirname(os.path.realpath(__file__))
        subprocess.check_call(
            ["less", f"{directory}/wordlettercount/wordlettercount.py"]
        )

    def view_spark_app_output(self):
        rds.show_db_contents(
            self._config.rds_host,
            self._config.rds_port,
            self._db_name,
            self._config.rds_username,
            self._config.rds_password,
        )

    def _run(self, args, **kwargs):
        dry = ["echo"] if self._dry_run else []
        return subprocess.run(dry + args, text=True, **kwargs)

    def _run_kops(self, args, **kwargs):
        return self._run(["kops", "--state", self._s3_bucket_path] + args, **kwargs)

    def _run_aws(self, args, **kwargs):
        return self._run(["aws"] + args, **kwargs)

    def _run_kubectl(self, args, **kwargs):
        return self._run(["kubectl"] + args, **kwargs)

    def _get_master_endpoint(self):
        output = self._run_kubectl(
            ["cluster-info"], capture_output=True, check=True
        ).stdout
        output = re.sub(r"\x1B\[[0-?]*[ -/]*[@-~]", "", output)  # Strip ANSI colours
        search_string = "Kubernetes master is running at "
        for line in output.splitlines():
            if line.startswith(search_string):
                return line[len(search_string) :]
        raise RuntimeError("No master")

    def _generate_random_string(self):
        return "".join(random.choices(string.ascii_lowercase, k=5))


def load_creds():
    path = pathlib.Path.home() / pathlib.Path(".aws/credentials")
    with path.open() as f:
        lines = f.readlines()
    access_key = None
    secret_key = None
    for line in lines:
        segs = line.strip().split("=")
        if len(segs) != 2:
            continue
        if segs[0] == "aws_access_key_id":
            access_key = segs[1]
        elif segs[0] == "aws_secret_access_key":
            secret_key = segs[1]
    return (access_key, secret_key)


def main():
    access_key, secret_key = load_creds()
    with Interface(access_key, secret_key) as interface:
        interface.run()


if __name__ == "__main__":
    main()
