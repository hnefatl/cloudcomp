#!/usr/bin/env python3

import aws
import tempfile
import subprocess

class Interface:
    def __init__(self, **kwargs):
        self._config = aws.ClusterConfig(**kwargs)
        self._run = True
        self._action_dict = {
            0: self.stop,
            1: self.edit_cluster_definition,
            11: self.print_cluster_definition,
        }

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass
        
    def run(self):
        while self._run:
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
        filepath = None
        with tempfile.NamedTemporaryFile(mode="w+", delete=False) as file:
            filepath = file.name
            file.write(self._config.json_show())

        # Let the user edit the JSON config
        result = subprocess.call("$EDITOR " + filepath, shell=True)

        if result != 0:
            print("Failed to edit file")
            return
        with open(filepath, mode="r+") as file:
            contents = file.read()
            self._config.json_load(contents)

def main():
    with Interface(
        key_path="/home/keith/.ssh/aws.pem",
        security_group_id="sg-09895ef455657c2e0"
    ) as interface:
        interface.run()

if __name__ == "__main__":
    main()