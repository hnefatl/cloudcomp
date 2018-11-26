#!/usr/bin/env python3.7

import boto3
import s3helper
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
import db


class Interface:
    def __init__(self, aws_access_key, aws_secret_key):
        self._config = clusterconfig.ClusterConfig()
        self._aws_access_key = aws_access_key
        self._aws_secret_key = aws_secret_key
        self._rds_instance_id_prefix = "group8"
        self._rds_username = "foo"
        self._rds_password = "hkXxep0A4^JZ1!H"
        self._rds_db_name = "kc506_rc691_CloudComputingCoursework"
        self._running = True
        self._dry_run = False
        self._cluster_started = False
        self._s3_bucket_url = None

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
            "24": self.validate_cluster_wait,
            "3": self.view_cluster,
            "31": self.get_admin_password,
            "32": self.get_admin_service_token,
            "4": self.delete_cluster,
            "5": self.run_spark_app,
            "51": self.view_spark_app,
            "52": self.view_spark_app_output,
            "6": self.run_custom_app,
            "61": self.view_custom_app,
            "62": self.view_custom_app_output,
            "7": self.delete_rds_instance,
            "c": lambda: subprocess.check_call("clear"),
        }

    def __enter__(self):
        return self

    def __exit__(self, *_):
        # Don't automatically kill the cluster when we exit: useful for dev
        # if self._cluster_started:
        #    self.delete_cluster()
        if self._s3_bucket_url is not None:
            print(f"Leaving cluster running on {self._s3_bucket_url}")

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
        print("    24: Wait for the cluster to be valid")
        print("3: View the cluster")
        print("    31: Get the admin password")
        print("    32: Get the admin service account token")
        print("4: Delete the cluster")
        print("5: Run Spark WordLetterCount App")
        print("    51: View Spark App")
        print("    52: Show Output")
        print("6: Run Custom WordLetterCount App")
        print("    61: View Custom App")
        print("    62: Show Output")
        print("7: Delete RDS instance")

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

        self._s3_bucket_url = (
            f"s3://{self._config.s3_bucket_prefix}.{self._generate_random_string()}"
        )

        _, _, vpc_id = self._get_or_create_rds_instance()

        # Create resources
        print(f"Creating s3 bucket {self._s3_bucket_url} in {self._config.region}")
        self._run_aws(
            ["s3", "mb", self._s3_bucket_url, "--region", self._config.region]
        ).check_returncode()
        print("Uploading cluster settings to bucket")
        s3helper.upload_file(
            s3helper.get_bucket_from_s3_url(self._s3_bucket_url),
            "config.json",
            self._config.json_show().encode(),
        )
        print(
            f"Creating cluster: {self._config.cluster_name} in {self._config.kubernetes_zones}"
        )
        self._run_kops(
            [
                "create",
                "cluster",
                self._config.cluster_name,
                "--zones",
                self._config.kubernetes_zones,
                "--authorization",
                "AlwaysAllow",
                "--master-count",
                str(self._config.master_count),
                "--master-size",
                self._config.master_type,
                "--node-size",
                self._config.slave_type,
                "--node-count",
                str(self._config.slave_count),
                "--vpc",
                vpc_id,
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
        self._run_aws(["s3", "rb", self._s3_bucket_url, "--force"]).check_returncode()
        self._s3_bucket_url = None
        self._cluster_started = False

    def set_existing_cluster(self):
        self._s3_bucket_url = input(
            "Enter state store url (eg. s3://kubernetes.group8 or kubernetes.group8): "
        )
        if not self._s3_bucket_url.startswith("s3://"):
            self._s3_bucket_url = "s3://" + self._s3_bucket_url
        self._cluster_started = True
        print("Downloading cluster config")
        contents = s3helper.download_file(
            s3helper.get_bucket_from_s3_url(self._s3_bucket_url), "config.json"
        )
        self._config.json_load(contents.decode())

    def validate_cluster(self):
        if not self._cluster_started:
            print("Cluster not started")
            return

        self._run_kops(
            ["validate", "cluster", self._config.cluster_name]
        ).check_returncode()

    def validate_cluster_wait(self):
        # Loop until the cluster is successfully validated
        while True:
            try:
                self.validate_cluster()
                return
            except subprocess.CalledProcessError:
                time.sleep(1)

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
        if not self._cluster_started:
            print("Cluster not started")
            return

        rds_host, rds_port, _ = self._get_or_create_rds_instance()

        input_url = input(
            "Enter s3 url to the input file (eg. s3a://kubernetes.group8/input.txt): "
        )
        if not re.match(r"s3a?://.*", input_url):
            print("Only s3 and s3a urls supported.")
            return

        print("Resetting spark database tables")
        db.initialise_instance(
            host=rds_host,
            port=rds_port,
            db_name=self._rds_db_name,
            username=self._rds_username,
            password=self._rds_password,
            table_suffix="spark",
        )
        master_endpoint = self._get_master_endpoint()
        print(f"Master endpoint: {master_endpoint}")
        print("Starting spark job")
        # Run the spark job
        subprocess.check_call(
            [
                "spark-submit",
                # General config
                "--master",
                f"k8s://{master_endpoint}",
                "--deploy-mode",
                "cluster",
                "--name",
                "wordlettercount",
                "--conf",
                f"spark.executor.instances={self._config.slave_count - 1}",
                "--conf",
                "spark.kubernetes.pyspark.pythonVersion=3",
                "--conf",
                "spark.kubernetes.container.image=docker.io/clgroup8/wordlettercount:latest",
                "--conf",
                "spark.kubernetes.container.image.pullPolicy=Always",
                "--conf",
                "spark.driver.cores=0.6",
                "--conf",
                "spark.kubernetes.executor.request.cores=0.6",
                # Script to run
                "file:///usr/spark-2.4.0/work-dir/wordlettercount.py",
                # Arguments to the script
                self._aws_access_key,
                self._aws_secret_key,
                rds_host,
                str(rds_port),
                self._rds_username,
                self._rds_password,
                self._rds_db_name,
                input_url,
            ]
        )

    def view_spark_app(self):
        directory = os.path.dirname(os.path.realpath(__file__))
        files = ["wordlettercount/wordlettercount.py"]
        for f in files:
            subprocess.check_call(["less", f"{directory}/{f}"])

    def view_spark_app_output(self):
        rds_host, rds_port, _ = self._get_or_create_rds_instance()
        db.show_db_contents(
            rds_host,
            rds_port,
            self._rds_db_name,
            self._rds_username,
            self._rds_password,
            "spark",
        )

    def run_custom_app(self):
        if not self._cluster_started:
            print("Cluster not started")
            return

        rds_host, rds_port, _ = self._get_or_create_rds_instance()

        input_url = input(
            "Enter s3 url to the input file (eg. s3a://kubernetes.group8/input.txt): "
        )
        if not re.match(r"s3a?://.*", input_url):
            print("Only s3 and s3a urls supported.")
            return
        chunk_size = input("Enter chunk size (or blank line for default chunk size): ")

        print("Resetting custom database tables")
        db.initialise_instance(
            host=rds_host,
            port=rds_port,
            db_name=self._rds_db_name,
            username=self._rds_username,
            password=self._rds_password,
            table_suffix="custom",
        )

        args = [
            "python",
            "wordlettercount-custom/master.py",
            input_url,
            rds_host,
            str(rds_port),
            self._config.region,
        ]
        env = os.environ.copy()
        env["AWS_ACCESS_KEY_ID"] = self._aws_access_key
        env["AWS_SECRET_ACCESS_KEY"] = self._aws_secret_key
        if len(chunk_size) > 0:
            args.append(chunk_size)
        print("Starting custom job")
        subprocess.check_call(args, env=env)

    def view_custom_app(self):
        directory = os.path.dirname(os.path.realpath(__file__))
        files = [
            "wordlettercount-custom/master.py",
            "wordlettercount-custom/mapper.py",
            "wordlettercount-custom/reducer.py",
        ]
        for f in files:
            subprocess.check_call(["less", f"{directory}/{f}"])

    def view_custom_app_output(self):
        rds_host, rds_port, _ = self._get_or_create_rds_instance()
        db.show_db_contents(
            rds_host,
            rds_port,
            self._rds_db_name,
            self._rds_username,
            self._rds_password,
            "custom",
        )

    def delete_rds_instance(self):
        print("Deleting RDS instance (may take a while)")
        rds_instance_id = f"{self._rds_instance_id_prefix}-{self._config.region}"
        rds.delete_entire_rds_instance(self._config.region, rds_instance_id)

    def _run(self, args, **kwargs):
        dry = ["echo"] if self._dry_run else []
        return subprocess.run(dry + args, text=True, **kwargs)

    def _run_kops(self, args, **kwargs):
        return self._run(["kops", "--state", self._s3_bucket_url] + args, **kwargs)

    def _run_aws(self, args, **kwargs):
        return self._run(["aws"] + args, **kwargs)

    def _run_kubectl(self, args, **kwargs):
        return self._run(["kubectl"] + args, **kwargs)

    def _get_or_create_rds_instance(self):
        instance_info = None
        rds_instance_id = f"{self._rds_instance_id_prefix}-{self._config.region}"
        try:
            instance_info = rds.get_instance_endpoint(
                self._config.region, rds_instance_id
            )
        except RuntimeError:
            print(
                f"Creating RDS instance in region {self._config.region} (may take a while)"
            )
            rds.create_entire_rds_instance(
                self._config.region,
                rds_instance_id,
                self._rds_username,
                self._rds_password,
            )
            instance_info = rds.get_instance_endpoint(
                self._config.region, rds_instance_id
            )

        if instance_info is None:
            raise RuntimeError("Failed to create RDS instance")
        rds_host, rds_port, vpc_id = (
            instance_info["host"],
            instance_info["port"],
            instance_info["vpc_id"],
        )
        print(
            f"Found RDS instance {rds_instance_id} in VPC {vpc_id} on {rds_host}:{rds_port}"
        )
        return (rds_host, rds_port, vpc_id)

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
        segs = list(map(lambda s: s.strip(), line.split("=")))
        if len(segs) != 2:
            continue
        if segs[0] == "aws_access_key_id":
            access_key = segs[1]
        elif segs[0] == "aws_secret_access_key":
            secret_key = segs[1]
    return (access_key, secret_key)


def main():
    access_key, secret_key = load_creds()
    if access_key is None:
        raise RuntimeError("Null access key")
    if secret_key is None:
        raise RuntimeError("Null secret key")
    with Interface(access_key, secret_key) as interface:
        interface.run()


if __name__ == "__main__":
    main()
