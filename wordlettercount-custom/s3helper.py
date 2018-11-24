import boto3
import re
from smart_open import smart_open


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
def get_file_length(bucket, filename):
    s3 = boto3.client("s3")
    return s3.head_object(Bucket=bucket, Key=filename)["ContentLength"]


# Returns the bytes in the given range of the remote file as a UTF-8 decoded string
def download_chunk(bucket, filename, chunk_range):
    s3 = boto3.client("s3")
    chunk_start, chunk_end = chunk_range
    range_string = f"bytes={chunk_start}-{chunk_end - 1}"  # Expects inclusive upper bound
    resp = s3.get_object(Bucket=bucket, Key=filename, Range=range_string)
    return resp["Body"].read().decode()


def upload_file(bucket, filename, data_bytes):
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=filename, Body=data_bytes)


def download_file(bucket, filename):
    s3 = boto3.client("s3")
    return s3.get_object(Bucket=bucket, Key=filename)["Body"].read()


# Generate a sequence of [start, end) byte ranges in the given file that can be read
# to guarantee that we'll get a valid utf-8 string from.
def get_chunks(file_url, ideal_chunk_size):
    range_start, range_end = (0, 0)
    for line in smart_open(file_url, mode="rb", encoding="utf-8"):
        range_end += len(line)
        if range_end - range_start >= ideal_chunk_size:
            yield (range_start, range_end)
            range_start = range_end
    if range_end > range_start:
        yield (range_start, range_end)
