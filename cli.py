#!/usr/bin/env python3.7

import clusterconfig
import subprocess
import sys
import string

import random
import os

# Configs:
# kops --state s3://kubernetes.group8.qngwz edit cluster
# kops --state s3://kubernetes.group8.qngwz get ig
# kops --state s3://kubernetes.group8.qngwz edit ig nodes
# kops --state s3://kubernetes.group8.qngwz edit ig master-eu-west-1a
# Sufficient to just pop these up? Or have to provide a subset of options and patch them in to the files?
# Only way to edit seems to be manually editing or providing a complete file.

class Interface:
    def __init__(self, config_path):
        self._running = True
        self._cluster_created = False
        self._dry_run = False
        self._cluster_name = "group8.cluster.k8s.local"
        self._default_zone = "eu-west-1a"
        # Generate a random s3 bucket name
        random_suffix = "".join(random.choices(string.ascii_lowercase, k=5))
        self._s3_bucket_name = f"kubernetes.group8.{random_suffix}"
        self._s3_bucket_path = "s3://" + self._s3_bucket_name
        # Map an input number to an action
        self._action_dict = {
            0:  self.stop,
            1:  self.edit_cluster_definition,
            11: self.print_cluster_definition,
            2:  self.start_cluster,
            4:  self.stop_cluster,
        }

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.stop_cluster()

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
        raise NotImplementedError()

    def edit_cluster_definition(self):
        zone_name = input(f'Enter zone name (leave blank for default zone "{self._default_zone}"): ')
        if zone_name == "":
            zone_name = self._default_zone
        print(f"Creating S3 bucket: {self._s3_bucket_path}")
        self._run_aws(["s3", "mb", self._s3_bucket_path]).check_returncode()
        print(f"Creating cluster: {self._cluster_name} in {zone_name}")
        self._run_kops(["create", "cluster", self._cluster_name, "--zones", zone_name, "--yes"]).check_returncode()
        print("Press enter to edit cluster definition:")
        self._run_kops(["edit", "cluster"]).check_returncode()
        print("Press enter to edit node definition:")
        self._run_kops(["edit", "ig", "nodes"]).check_returncode()
        print("Press enter to edit master definition:")
        self._run_kops(["edit", "ig", self._get_master_name()]).check_returncode()

    def _get_master_name(self):
        output = self._run_kops(["get", "ig"], capture_output=True).stdout
        for line in output.splitlines():
            # Each line is like "master-eu-west-1a Master ..."
            words = line.split()
            if words[1] == "Master":
                return words[0]
        raise RuntimeError("No master")

    def start_cluster(self):
        print("Running cluster")
        self._run_kops(["update"]).check_returncode()
    
    def stop_cluster(self):
        print("Deleting S3 bucket")
        self._run_aws(["s3", "rb", self._s3_bucket_path, "--force"]).check_returncode()

    def _run(self, args, **kwargs):
        dry = ["echo"] if self._dry_run else []
        return subprocess.run(dry + args, text=True, **kwargs)
    def _run_kops(self, args, **kwargs):
        return self._run(["kops", "--state", self._s3_bucket_path] + args, **kwargs)
    def _run_aws(self, args, **kwargs):
        return self._run(["aws"] + args, **kwargs)


def main():
    with Interface(config_path="config.json") as interface:
        interface.run()

if __name__ == "__main__":
    if sys.version_info < (3, 7, 0, "", 0):
        raise RuntimeError("Python >=3.7 required")
    main()