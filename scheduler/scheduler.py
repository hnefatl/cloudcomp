#! /usr/bin/env python3


class App:
    def __init__(self, name, master):
        self.name = name
        self._master = master  # TODO: Replace with regex?

    def is_master(self, app_name):
        if self._master != "" and self._master in app_name:
            return True
        return False


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
        self._nodes = set()  # TODO: Initialise to kubernetes nodes
        self.allocations = {app: set() for app in self._apps}

    def _unlabel(self, app, n):
        to_unallocate = set()
        for node in self.allocations[app]:
            if n == 0:
                break
            elif app.is_master(node):  # TODO: node should be name of pod on node
                continue
            else:
                n -= 1
                to_unallocate.add(node)

        if n == 0:  # Unallocation request met
            # TODO: k8s request to remove label from nodes
            self.allocations[app] = self.allocations[app].difference(to_unallocate)
            self._nodes = self._nodes.union(to_unallocate)
            return to_unallocate
        else:
            return []

    @staticmethod
    def _drain(nodes):
        for node in nodes:
            # TODO: k8s request to drain nodes
            pass

    def _label(self, app, n):
        to_allocate = set()
        for node in self._nodes:
            if n == 0:
                break
            n -= 1
            to_allocate.add(node)

        for node in to_allocate:
            # TODO: k8s request to tag node with label
            pass

        return to_allocate

    def unallocate(self, app, n=1, drain=True):
        assert app in self.allocations
        nodes = self._unlabel(self, app, n)
        if len(nodes) == 0:
            # TODO: Handle impossible request
            pass
        if drain:
            Scheduler._drain(nodes)
        return nodes

    def allocate(self, app, n=1):
        assert app in self.allocations
        nodes = self._label(self, app, n)
        return nodes

    def transfer(self, app1, app2, n=1, drain=True):
        self.unallocate(app1, n, drain)  # Handle impossible request
        self.allocate(app2, n)
