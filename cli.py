#!/usr/bin/env python3.7

import subprocess
import string
import random
import tempfile

import clusterconfig

# Configs:
# kops --state s3://kubernetes.group8.qngwz edit cluster
# kops --state s3://kubernetes.group8.qngwz get ig
# kops --state s3://kubernetes.group8.qngwz edit ig nodes
# kops --state s3://kubernetes.group8.qngwz edit ig master-eu-west-1a
# Sufficient to just pop these up? Or have to provide a subset of options and patch them in to the files?
# Only way to edit seems to be manually editing or providing a complete file.

class Interface:
    def __init__(self):
        self._config = clusterconfig.ClusterConfig()
        self._running = True
        self._dry_run = False
        self._cluster_started = False
        self._s3_bucket_path = None
        # Map an input number to an action
        self._action_dict = {
            0:  self.stop,
            1:  self.edit_cluster_definition,
            11: self.print_cluster_definition,
            2:  self.start_cluster,
            21: self.validate_cluster,
            3:  self.view_cluster,
            4:  self.delete_cluster,
        }

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.delete_cluster()

    def run(self):
        while self._running:
            self.show_menu()
            action = self.get_action()
            action()

    def show_menu(self):
        print("0: Exit")
        print("1: Define a Kubernetes Cluster")
        print("    11: Review the cluster definition")
        print("2: Launch the cluster on AWS")
        print("    21: Validate the cluster")
        print("    22: Deploy the Kubernetes web dashboard")
        print("    23: Access the Kubernetes web dashboard")
        print("3: View the cluster")
        print("    31: Get the admin password")
        print("    32: Get the admin service account token")
        print("4: Delete the cluster")

    def get_action(self):
        try:
            selection = int(input("Please enter your choice: "))
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

        self._s3_bucket_path = self._generate_random_bucket_path()

        # Create resources
        print(f"Creating s3 bucket {self._s3_bucket_path}")
        self._run_aws(["s3", "mb", self._s3_bucket_path]).check_returncode()
        print(f"Creating cluster: {self._config.cluster_name} in {self._config.zone}")
        self._run_kops([
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
        ]).check_returncode()

        # Run resources
        print("Running cluster")
        self._run_kops(["update", "cluster", self._config.cluster_name, "--yes"]).check_returncode()
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
        self._cluster_started = False

    def validate_cluster(self):
        if not self._cluster_started:
            print("Cluster not started")
            return

        self._run_kops(["validate", "cluster", self._config.cluster_name]).check_returncode()

    def view_cluster(self):
        if not self._cluster_started:
            print("Cluster not started")
            return
        self._run_kops(["get", "cluster", self._config.cluster_name]).check_returncode()
        self._run_kops(["get", "ig"]).check_returncode()

    def _run(self, args, **kwargs):
        dry = ["echo"] if self._dry_run else []
        return subprocess.run(dry + args, text=True, **kwargs)
    def _run_kops(self, args, **kwargs):
        return self._run(["kops", "--state", self._s3_bucket_path] + args, **kwargs)
    def _run_aws(self, args, **kwargs):
        return self._run(["aws"] + args, **kwargs)

    def _get_master_name(self):
        output = self._run_kops(["get", "ig"], capture_output=True, check=True).stdout
        for line in output.splitlines():
            # Each line is like "master-eu-west-1a Master ..."
            words = line.split()
            if words[1] == "Master":
                return words[0]
        raise RuntimeError("No master")

    def _generate_random_bucket_path(self):
        random_suffix = "".join(random.choices(string.ascii_lowercase, k=5))
        return f"s3://{self._config.s3_bucket_prefix}.{random_suffix}"


def main():
    with Interface() as interface:
        interface.run()

if __name__ == "__main__":
    main()