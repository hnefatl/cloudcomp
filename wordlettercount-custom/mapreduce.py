#! /usr/bin/env python3
from kubernetes import client
import os


# Factory for starting Kubernetes jobs
# Assigns an increasing execution unique id to all
# created jobs.
class JobFactory:
    next_id = 0

    def __init__(self, client, image):
        self._client = client
        self._image = image
        # Initialise a template kubernetes job

    def _create_job(self, name, *args):
        return client.V1Job(
            metadata=client.V1ObjectMeta(name=name),
            spec=client.V1JobSpec(
                template=client.V1PodTemplateSpec(
                    spec=client.V1PodSpec(
                        restart_policy="OnFailure",
                        containers=[
                            client.V1Container(
                                image=self._image,
                                name=name,
                                env=[
                                    client.V1EnvVar(
                                        name="AWS_ACCESS_KEY_ID",
                                        value=os.environ["AWS_ACCESS_KEY_ID"],
                                    ),
                                    client.V1EnvVar(
                                        name="AWS_SECRET_ACCESS_KEY",
                                        value=os.environ["AWS_SECRET_ACCESS_KEY"],
                                    ),
                                    client.V1EnvVar(name="JOB_ID", value=name)
                                ],
                                args=args,
                            )
                        ],
                    )
                )
            ),
        )

    def new_job(self, id_prefix, *args):
        job_id = JobFactory.next_id
        JobFactory.next_id += 1
        job_name = f"{id_prefix}-{job_id}"
        return self._client.create_namespaced_job(
            "default", self._create_job(job_name, *args)
        )


class JobList:
    def __init__(self, running, completed):
        self.running = running
        self.completed = completed

    def __str__(self):
        return f"JobList({len(self.running)}, {len(self.completed)})"

    def __repr__(self):
        return self.__str__()


# MapReduce represents the cluster state of an inprogress MapReduce
class MapReduce:
    def __init__(self, app_id, client, ranges, mapper_image, reducer_image):
        self._id = app_id
        self._client = client
        self._mapper_factory = JobFactory(client, mapper_image)
        self._reducer_factory = JobFactory(client, reducer_image)
        self.ranges = ranges
        self.mappers = JobList([], [])
        self.reducers = {tag: JobList([], []) for tag in ranges}

    def start_mapper(self, *args):
        mapper = self._mapper_factory.new_job(f"mapper-{self._id}", *args)
        self.mappers.running.append(mapper)

    def start_reducer(self, tag, *args):
        assert tag in self.ranges
        reducer = self._reducer_factory.new_job(f"reducer-{self._id}", *args)
        self.reducers[tag].running.append(reducer)

    @staticmethod
    def _partition(l, f):
        a, b = ([], [])
        for item in l:
            if f(item):
                a.append(item)
            else:
                b.append(item)
        return a, b

    def update_state(self):
        # Get names of all successful jobs from Kubernetes
        job_list = {
            job.metadata.name
            for job in self._client.list_namespaced_job("default").items
            if job.status is not None
            and job.status.succeeded is not None
            and job.status.succeeded > 0
        }

        # partition running mappers on inclusion in job_list
        completed, running = MapReduce._partition(
            self.mappers.running, lambda x: x.metadata.name in job_list
        )
        self.mappers.running = running
        self.mappers.completed.extend(completed)

        # for each tag, partiion running reducers on inclusion in job_list
        for tag in self.ranges:
            completed, running = MapReduce._partition(
                self.reducers[tag].running, lambda x: x.metadata.name in job_list
            )
            self.reducers[tag].running = running
            self.reducers[tag].completed.extend(completed)

    # Checks if there are any running taks in this MapReduce
    def is_active(self):
        return (
            len(self.mappers.running)
            + sum((len(self.reducers[tag].running) for tag in self.ranges))
            != 0
        )
