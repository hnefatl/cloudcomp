#!/usr/bin/env python3
import sys

from kubernetes import client, config
import random
import time
import string
from mapreduce import MapReduce
import s3helper
import boto3

MAPPER_IMAGE = "clgroup8/mapper:latest"
REDUCER_IMAGE = "clgroup8/reducer:latest"
RANGES = ["a-l", "m-z"]
NUM_MAPPERS_TO_REDUCERS = len(RANGES)
NUM_REDUCERS_TO_REDUCERS = len(RANGES)
EVENT_LOOP_UPDATE_INTERVAL = 1


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
    if tag == "":
        return f"{bucket}/{job}"
    else:
        return f"{bucket}/{job}/{tag}"


def main():
    if len(sys.argv) not in [2, 3]:
        print("Usage: master.py <input-url> [chunk-size]")
        sys.exit(1)

    input_url = sys.argv[1]
    chunk_size = 100
    if len(sys.argv) == 3:
        chunk_size = int(sys.argv[2])

    kube = authenticate_kubernetes()
    bucket_id = "".join(random.choices(string.ascii_lowercase, k=5))
    bucket = "s3://group8.wlcc.{}".format(bucket_id)

    # Create bucket
    client = boto3.client("s3")
    client.create_bucket(
        Bucket=bucket[5:], CreateBucketConfiguration={"LocationConstraint": "EU"}
    )

    mr = MapReduce(bucket_id, kube, RANGES, MAPPER_IMAGE, REDUCER_IMAGE)
    for (c1, c2) in s3helper.get_chunks(input_url, chunk_size):
        mr.start_mapper(input_url, bucket, str(c1), str(c2), ",".join(RANGES))
    work_done = False
    state = 0

    # Event loop updates state and looks for possible reduction
    # Terminates when state isn't changed, no reducers are started
    # and there are no running jobs.
    while work_done or mr.is_active():
        work_done = False
        mr.update_state()
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
                    ",".join(
                        get_s3_url(bucket, mapper.metadata.name, tag)
                        for mapper in to_reduce
                    ),
                    bucket,
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
                        get_s3_url(bucket, reducer.metadata.name, "")
                        for reducer in to_reduce
                    ),
                    bucket,
                )
                work_done = True
        time.sleep(EVENT_LOOP_UPDATE_INTERVAL)

    # s3_bucket = boto3.resource("s3").Bucket(bucket[5:])
    # s3_bucket.objects.all().delete()
    # s3_bucket.delete()


if __name__ == "__main__":
    main()
