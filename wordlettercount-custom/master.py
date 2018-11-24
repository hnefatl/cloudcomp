#!/usr/bin/env python3
import sys

from kubernetes import client, config
import random
import time
import string
from mapreduce import MapReduce

MAPPER_IMAGE = "clgroup8/test:latest"
REDUCER_IMAGE = "clgroup8/test:latest"
RANGES = ["a-l", "m-z"]
NUM_MAPPERS_TO_REDUCERS = len(RANGES)
NUM_REDUCERS_TO_REDUCERS = len(RANGES)
EVENT_LOOP_UPDATE_INTERVAL = 1


# Splits a list of urls into chunks
# TODO(rc691): Replace with kc506 implementation
def get_chunks(url):
    return ["a", "b", "c", "d"]


# Authenticates with kubernetes and returns a new client
# TODO(rc691): Support authentication on node in cluster
def authenticate_kubernetes():
    config.load_kube_config()
    return client.BatchV1Api()


def take_at_most_n(l, n):
    if len(l) > n:
        return l[:n], l[n:]
    else:
        return l, []


def get_s3_url(bucket, job, tag):
    return "{}/{}/{}".format(bucket, job, tag)


def main():
    if len(sys.argv) != 2:
        print("Usage: master.py <input-url>")
        sys.exit(1)

    kube = authenticate_kubernetes()
    # TODO(rc691): Create an actual S3 bucket
    bucket_id = "".join(random.choices(string.ascii_lowercase, k=5))
    bucket = "s3://group8.wlcc.{}".format(bucket_id)

    mr = MapReduce(bucket_id, kube, RANGES, MAPPER_IMAGE, REDUCER_IMAGE)
    for chunk in get_chunks(sys.argv[1]):
        mr.start_mapper(chunk)  # TODO: modify to pass actual parameters
    state_updated = True
    work_done = False
    state = 0

    # Event loop updates state and looks for possible reduction
    # Terminates when state isn't changed, no reducers are started
    # and there are no running jobs.
    while state_updated or work_done or mr.is_active():
        work_done = False
        state_updated = mr.update_state()
        print("State", state, "-", mr.mappers, mr.reducers)
        state += 1
        # Reduce mappers before other reducers
        # Logic behind explicit order is that the result of mappers are not
        # as far along the reduction process, so will need more time to be processed.
        # Termination condition is that there are not enough completed mappers
        # to start a new reducer with AND the mappers which are completed are
        # not the last few.
        while len(mr.mappers.completed) >= NUM_MAPPERS_TO_REDUCERS or (
            len(mr.mappers.running) == 0 and len(mr.mappers.completed) > 0
        ):
            to_reduce, remaining = take_at_most_n(
                mr.mappers.completed, NUM_MAPPERS_TO_REDUCERS
            )
            mr.mappers.completed = remaining
            for tag in RANGES:
                mr.start_reducer(
                    tag,
                    ",".join([get_s3_url(bucket, mapper, tag) for mapper in to_reduce]),
                )
                work_done = True

        # Reduce multiple reducers when they are compatible
        # Termination condition is slightly different because the final completed
        # reducer does not need to be reduced.
        for tag in RANGES:
            while len(mr.reducers[tag].completed) >= NUM_REDUCERS_TO_REDUCERS or (
                len(mr.reducers[tag].running) == 0
                and len(mr.reducers[tag].completed) > 1
            ):
                to_reduce, remaining = take_at_most_n(
                    mr.reducers[tag].completed, NUM_REDUCERS_TO_REDUCERS
                )
                mr.reducers[tag].completed = remaining
                mr.start_reducer(
                    tag,
                    ",".join(
                        [get_s3_url(bucket, reducer, tag) for reducer in to_reduce]
                    ),
                )
                work_done = True
        time.sleep(EVENT_LOOP_UPDATE_INTERVAL)


if __name__ == "__main__":
    main()
