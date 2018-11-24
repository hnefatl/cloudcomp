import collections
import s3helper
import sys
import os
import json


def get_json(url):
    bucket, filename = s3helper.get_bucket_and_file(url)
    return json.loads(s3helper.download_file(bucket=bucket, filename=filename).decode())


# Merge a list of (k, v) pairs into a dictionary mapping ks to vs
def add_list_to_dict(input, output):
    inputlist = input.items() if isinstance(input, dict) else input
    for (k, v) in inputlist:
        output[k] += v


def merge(inputs):
    output = {
        "word": collections.defaultdict(int),
        "letter": collections.defaultdict(int),
    }
    for input in inputs:
        add_list_to_dict(input["word"], output["word"])
        add_list_to_dict(input["letter"], output["letter"])
    return output


def main():
    if len(sys.argv) != 3:
        raise RuntimeError(
            "Usage: reducer <input file urls> <output bucket url>\n"
            + "where the input file urls are a comma separated list of s3:// urls and the output url is a s3:// url."
        )
    input_urls = sys.argv[1].split(",")
    out_bucket = sys.argv[2][5:]
    out_file = os.environ["JOB_ID"]

    inputs = [get_json(url) for url in input_urls]
    outputs = merge(inputs)
    json_output = json.dumps(outputs)

    s3helper.upload_file(
        bucket=out_bucket, filename=out_file, data_bytes=json_output.encode()
    )


if __name__ == "__main__":
    main()
