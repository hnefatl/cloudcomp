#!/usr/bin/env python3

import boto3
import re


# Splits an S3 url into a bucket name and file name, eg.
# "s3a://group8.samples/foo/bar.txt" into ("group8.samples", "foo/bar.txt")
def get_bucket_and_file(file_url):
    # First capture group gets any characters after the protocol until the first
    # forward slash (the bucket name), second captures everything after the first
    # forward slash (the file path)
    match = re.match(r"s3a?://([^/]+)/(.+)", file_url)
    if match is None:
        raise RuntimeError(f"Invalid file url: {file_url}")
    return (match.group(1), match.group(2))


# Get the length of a given file in a given bucket in bytes
def get_file_length(bucket, filename, region):
    s3 = boto3.client("s3", region_name=region)
    return s3.head_object(Bucket=bucket, Key=filename)["ContentLength"]


# Returns a list of [start, end] pairs indicating chunk start/end positions in bytes.
# All indices x returned by this function satisfy 0 <= x < length.
# Note the pairs are inclusive on both ends: we return eg. [(0,99),(100,199),...]
def get_chunk_indices(bucket, filename, chunk_bytes, region):
    length = get_file_length(bucket, filename, region)
    for i in range(0, length, chunk_bytes):
        yield (i, min(length, i + chunk_bytes) - 1)


# Returns the bytes in the given range of the remote file as a UTF-8 decoded string
def download_chunk(bucket, filename, chunk_range, region):
    s3 = boto3.client("s3", region_name=region)
    response = s3.get_object(
        Bucket=bucket, Key=filename, Range=f"bytes={chunk_range[0]}-{chunk_range[1]}"
    )
    return response["Body"].read().decode()
