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


# Returns a list of [start, end) pairs indicating chunk start/end positions in bytes.
# All indices x returned by this function satisfy 0 <= x <= length.
def get_chunk_indices(file_length, chunk_bytes, region):
    for i in range(0, file_length, chunk_bytes):
        yield (i, min(file_length, i + chunk_bytes))


# Returns the bytes in the given range of the remote file as a UTF-8 decoded string
def download_chunk(bucket, filename, chunk_range, region):
    s3 = boto3.client("s3", region_name=region)
    range_string = f"bytes={chunk_range[0]}-{chunk_range[1] - 1}" # Inclusive upper bound
    resp = s3.get_object(Bucket=bucket, Key=filename, Range=range_string)
    return resp["Body"].read().decode()
