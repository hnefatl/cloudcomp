#!/usr/bin/env python3.7

import asyncio
import tempfile
import subprocess
import sys

from cluster import Cluster, AlreadyStartedException
from clusterconfig import ClusterConfig

class Interface:
    def __init__(self, config_path):
        self._config_path = "config.json"
        self._config = ClusterConfig(config_path)
        self._cluster = Cluster(self._config)
        self._run = True
        # Map an input number to a *function or async function* that performs some action
        self._action_dict = {
            0: self.stop,
            1: self.edit_cluster_definition,
            11: self.print_cluster_definition,
            2: self.start_cluster,
            4: self.stop_cluster,
            5: self._ssh_test,
        }

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self._cluster.stop()

    async def run(self):
        while self._run:
            self.show_menu()
            action = self.get_action()
            if asyncio.iscoroutinefunction(action):
                await action()
            else:
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
        print("5: Test parallel SSH connections")

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
        self._run = False

    def invalid_entry(self):
        print("Invalid entry")

    def print_cluster_definition(self):
        print(self._config.json_show())

    def edit_cluster_definition(self):
        # Let the user edit the JSON config
        result = subprocess.call("$EDITOR " + self._config_path, shell=True)
        if result != 0:
            print("Failed to edit file")
            return

        self._config.json_load_path(self._config_path)

    async def start_cluster(self):
        try:
            await self._cluster.start(self._config)
        except AlreadyStartedException:
            print("Cluster already started")
    
    def stop_cluster(self):
        self._cluster.stop()

    async def _ssh_test(self):
        print("Command: " + str(await self._cluster.run_ssh_on_all("hostname ; date '+%X' ; sleep 10 ; date '+%X'")))
        print("Script: " + str(await self._cluster.run_script_on_all("sshdemo.sh")))


async def main():
    with Interface(config_path="config.json") as interface:
        await interface.run()

if __name__ == "__main__":
    if sys.version_info < (3, 7, 0, "", 0):
        raise RuntimeError("Python >=3.7 required")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())
    loop.close()