#! /usr/bin/env python3
from kubernetes import client


# An App is defined by a name, and an identifier to uniquely match the master node (if any)
class App:
    def __init__(self, name, master):
        self.name = name
        self._master = master  # TODO: Replace with regex?

    def is_master(self, app_name):
        if self._master != "" and self._master in app_name:
            return True
        return False

    def __repr__(self):
        if self._master == "":
            return f"Application({self.name})"
        else:
            return f"Application({self.name}, {self._master})"


# AllocationError is the error thrown by the Scheduler when the requested allocation is impossible
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
        self._deallocated = {
            node.metadata.name
            for node in self._client.list_node(
                label_selector="kubernetes.io/role=node"
            ).items
        }
        self._allocated = {app.name: set() for app in self._apps}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        for node in self._allocated_iter():
            self._clear_label(node)

    def _allocated_iter(self):
        for nodes in self._allocated:
            for node in nodes:
                yield node

    def _clear_label(self, node):
        self._client.patch_node(node, {"metadata": {"labels": {"app": None}}})

    # Call k8s API and sets the node label to the specified app
    def _set_app_on_node(self, node, app_name):
        self._client.patch_node(node, {"metadata": {"labels": {"app": app_name}}})

    # Deallocate n nodes from app, making sure that the master is not scheduled on the deallocated nodes
    # This does not actually deallocate, but only modifies the schedulers internal state (required for an optimisation is transfer)
    def _deallocate_local(self, app, n):
        to_deallocate = set()
        num_req = n  # Take a copy in case exception is thrown
        for node in self._allocated[app.name]:
            if n == 0:
                break

            podList = self._client.list_namespaced_pod(
                "default", field_selector=f"spec.nodeName={node}"
            )
            if any(app.is_master(pod.metadata.name) for pod in podList.items):
                continue
            else:
                n -= 1
                to_deallocate.add(node)

        if n == 0:  # Deallocation request met
            self._allocated[app.name] = self._allocated[app.name].difference(
                to_deallocate
            )
            self._deallocated = self._deallocated.union(to_deallocate)
            return to_deallocate
        else:
            raise AllocationError(
                f"Deallocation request for {num_req} nodes; {app.name} has {num_req - n} nodes that may be deallocated"
            )

    # Deallocates n nodes from the app
    def _deallocate(self, app, n):
        nodes = self._deallocate_local(app, n)
        for node in nodes:
            self._set_app_on_node(node, "")
        return nodes

    # Deletes pods belonging to app from the nodes
    def _drain(self, app, nodes):
        for node in nodes:
            podList = self._client.list_namespaced_pod(
                "default",
                label_selector=f"app={app.name}",
                field_selector=f"spec.nodeName={node}",
            )
            for pod in podList.items:
                self._client.delete_namespaced_pod(
                    pod.metadata.name, "default", body=client.V1DeleteOptions()
                )

    @property
    def deallocated(self):
        return self._deallocated

    @property
    def allocated(self):
        return self._allocated

    @property
    def apps(self):
        return self._apps

    # External wrapper around _deallocate which also optionally drains nodes
    def deallocate(self, app, n=1, drain=True):
        assert app.name in self._allocated
        nodes = self._deallocate(app, n)
        if drain:
            self._drain(app, nodes)
        return nodes

    # Allocates n nodes to the app
    def allocate(self, app, n=1):
        assert app.name in self._allocated
        to_allocate = set()
        num_req = n
        for node in self._deallocated:
            if n == 0:
                break
            n -= 1
            to_allocate.add(node)
        if n > 0:
            raise AllocationError(
                f"Allocation request for {num_req} nodes to {app.name}; Only {num_req - n} deallocated nodes exist"
            )

        for node in to_allocate:
            self._set_app_on_node(node, app.name)

        self._deallocated = self._deallocated.difference(to_allocate)
        self._allocated[app.name] = self._allocated[app.name].union(to_allocate)

        return to_allocate

    # Transfers n nodes from app1 -> app2
    # In order to optimise number of requests, only uses deallocate_local, since
    # allocate replaces the node labels anyway
    def transfer(self, app1, app2, n=1, drain=True):
        assert app1.name in self._allocated
        assert app2.name in self._allocated
        transferred = self._deallocate_local(app1, n)
        # Allocate first to allow apps to be scheduled ASAP
        self.allocate(app2, n)
        if drain:
            # Do not need to drain a node before allocation as drain only deletes pods belonging to a specified app
            self._drain(app1, transferred)
        return transferred
