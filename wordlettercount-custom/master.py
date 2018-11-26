#!/usr/bin/env python3

import sys
from kubernetes import client, config
from math import ceil, floor
import random
import time
import string
import threading
import json
from mapreduce import MapReduce
import boto3

import s3helper
import db

MAPPER_IMAGE = "clgroup8/mapper:latest"
REDUCER_IMAGE = "clgroup8/reducer:latest"
RDS_USERNAME = "foo"
RDS_PASSWORD = "hkXxep0A4^JZ1!H"
RDS_DB_NAME = "kc506_rc691_CloudComputingCoursework"
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
    if len(sys.argv) not in [5, 6]:
        print(
            "Usage: master.py <input-url> <rds-hostname> <rds-port> <region> [chunk-size]"
        )
        sys.exit(1)

    input_url = sys.argv[1]
    rds_host = sys.argv[2]
    rds_port = int(sys.argv[3])
    region = sys.argv[4]
    chunk_size = 50_000_000
    if len(sys.argv) == 6:
        chunk_size = int(sys.argv[5])
    if chunk_size <= 0:
        raise RuntimeError(f"Chunk size must be a positive number: got {chunk_size}")

    kube = authenticate_kubernetes()

    bucket_id = "".join(random.choices(string.ascii_lowercase, k=5))
    bucket_url = f"s3://group8.wlcc.{bucket_id}"
    bucket_name = s3helper.get_bucket_from_s3_url(bucket_url)
    print("Creating temporary bucket")
    with s3helper.temporary_bucket(bucket_url, region):
        mr = MapReduce(bucket_id, kube, RANGES, MAPPER_IMAGE, REDUCER_IMAGE)
        work_done = False
        state = 0

        # Computing chunk sizes is slow: we want to compute it in the background while
        # # spinning up mappers, and without preventing us from reducing the output of the mappers
        def spawn_mappers():
            for c1, c2 in s3helper.get_chunks(input_url, chunk_size):
                mr.start_mapper(
                    input_url, bucket_url, str(c1), str(c2), ",".join(RANGES)
                )

        mapper_spawner_thread = threading.Thread(target=spawn_mappers)
        print("Starting to spawn mappers")
        mapper_spawner_thread.start()

        # Event loop updates state and looks for possible reduction
        # Terminates when state isn't changed, no reducers are started
        # and there are no running jobs.
        while work_done or mr.is_active() or mapper_spawner_thread.is_alive():
            work_done = False
            mr.update_state()
            print(
                f"State {state} - Mappers: [{mr.mappers}]    Reducers: [{mr.reducers}]"
            )
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
                            get_s3_url(bucket_url, mapper.metadata.name, tag)
                            for mapper in to_reduce
                        ),
                        bucket_url,
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
                            get_s3_url(bucket_url, reducer.metadata.name, "")
                            for reducer in to_reduce
                        ),
                        bucket_url,
                    )
                    work_done = True
            time.sleep(EVENT_LOOP_UPDATE_INTERVAL)

        print("Processing reducer outputs")
        # Collect the reducer outputs into a single dictionary
        output = {"word": [], "letter": []}
        for tag in RANGES:
            if len(mr.reducers[tag].completed) < 1:
                continue  # It's valid for the input to contain no letters in a range
            elif len(mr.reducers[tag].completed) > 1:
                raise RuntimeError(
                    f"Expected exactly one reducer for {tag}: got {mr.reducers[tag]}"
                )
            final_reducer_id = mr.reducers[tag].completed[0].metadata.name
            reducer_output = json.loads(
                s3helper.download_file(bucket_name, final_reducer_id).decode()
            )
            output["word"].extend(reducer_output["word"].items())
            output["letter"].extend(reducer_output["letter"].items())

        # Sort outputs: decreasing by frequency, increasing by word
        for r in output:
            output[r].sort(key=lambda x: x[0])
            output[r].sort(key=lambda x: x[1], reverse=True)

        print("Writing results to database")
        write_to_db(rds_host, rds_port, output)


def write_to_db(host, port, data):
    def process_categories(subdata):
        length = len(subdata)
        # Category boundaries. In the sample outputs, the ranges are inclusive on both sides.
        # Have to subtract one from the lower bounds to get the same output
        popular_u = int(ceil(0.05 * length))
        common_l = int(floor(0.475 * length)) - 1
        common_u = int(ceil(0.525 * length))
        rare_l = int(floor(0.95 * length)) - 1

        for r in range(0, popular_u):
            yield (r + 1, subdata[r][0], "popular", subdata[r][1])
        for r in range(common_l, common_u):
            yield (r + 1, subdata[r][0], "common", subdata[r][1])
        for r in range(rare_l, length):
            yield (r + 1, subdata[r][0], "rare", subdata[r][1])

    word_data, letter_data = (
        process_categories(data["word"]),
        process_categories(data["letter"]),
    )

    with db.pymysql_connect(
        host=host, port=port, user=RDS_USERNAME, password=RDS_PASSWORD, db=RDS_DB_NAME
    ) as connection, connection.cursor() as cursor:
        cursor.executemany("INSERT INTO words_custom VALUES (%s,%s,%s,%s)", word_data)
        cursor.executemany(
            "INSERT INTO letters_custom VALUES (%s,%s,%s,%s)", letter_data
        )


if __name__ == "__main__":
    main()
