#! /usr/bin/env python3
from kubernetes import client


class App:
    def __init__(self, name, master):
        self.name = name
        self._master = master  # TODO: Replace with regex?

    def is_master(self, app_name):
        if self._master != "" and self._master in app_name:
            return True
        return False


class AllocationError(Exception):
    pass


# A class to represent the state of the Scheduler
# This represents a collection of applications and
# the current nodes allocated to the applications
# Applications need to be configured to have unique
# names and only allow scheduling on nodes labelled
# with the name.
# Input:
# client - A Kubernetes client authorised
# apps - a list of app names
class Scheduler:
    def __init__(self, client, apps):
        self._client = client
        self._apps = apps
        self._unallocated = {
            node.metadata.name
            for node in self._client.list_node(
                label_selector="kubernetes.io/role=node"
            ).items
        }
        self.allocations = {app.name: set() for app in self._apps}

    def _set_app_on_node(self, node, app_name):
        self._client.patch_node(node, {"metadata": {"labels": {"app": app_name}}})

    def _unallocate_local(self, app, n):
        to_unallocate = set()
        num_req = n  # Take a copy in case exception is thrown
        for node in self.allocations[app.name]:
            if n == 0:
                break

            podList = self._client.list_namespaced_pod(
                "default", field_selector=f"spec.nodeName={node}"
            )
            if any(app.is_master(pod.metadata.name) for pod in podList.items):
                continue
            else:
                n -= 1
                to_unallocate.add(node)

        if n == 0:  # Unallocation request met
            self.allocations[app.name] = self.allocations[app.name].difference(
                to_unallocate
            )
            self._unallocated = self._unallocated.union(to_unallocate)
            return to_unallocate
        else:
            raise AllocationError(
                f"Unallocation request for {num_req} nodes; {app.name} has {num_req - n} nodes that may be unallocated"
            )

    def _unallocate(self, app, n):
        nodes = self._unallocate_local(app, n)
        for node in nodes:
            self._set_app_on_node(node, "")
        return nodes

    def _drain(self, app, nodes):
        for node in nodes:
            podList = self._client.list_namespaced_pod(
                "default",
                label_selector=f"app={app.name}",
                field_selector=f"spec.nodeName={node}",
            )
            for pod in podList.items:
                self._client.delete(
                    pod.metadata.name, "default", body=client.V1DeleteOptions()
                )

    def _allocate(self, app, n):
        to_allocate = set()
        num_req = n
        for node in self._unallocated:
            if n == 0:
                break
            n -= 1
            to_allocate.add(node)
        if n > 0:
            raise AllocationError(
                f"Allocation request for {num_req} nodes to {app.name}; Only {num_req - n} unallocated nodes exist"
            )

        for node in to_allocate:
            self._set_app_on_node(node, app.name)

        self._unallocated = self._unallocated.difference(to_allocate)
        self.allocations[app.name] = self.allocations[app.name].union(to_allocate)

        return to_allocate

    def num_unallocated(self):
        return len(self._unallocated)

    def unallocate(self, app, n=1, drain=True):
        assert app.name in self.allocations
        nodes = self._unallocate(app, n)
        if drain:
            self._drain(app, nodes)
        return nodes

    def allocate(self, app, n=1):
        assert app.name in self.allocations
        nodes = self._allocate(app, n)
        return nodes

    def transfer(self, app1, app2, n=1, drain=True):
        assert app1.name in self.allocations
        assert app2.name in self.allocations
        transferred = None
        if drain:
            transferred = self.unallocate(app1, n, drain)
            self.allocate(app2, n)
        else:
            transferred = self.unallocate_local(app1, n)
            self.allocate(app2, n)
        return transferred
