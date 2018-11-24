import s3helper
import sys
import re
import json
import itertools
import os


delims = list(' \t\r\v\f,.;:?!"()[]{}-_')
re_split = re.compile(r"|".join(map(re.escape, delims)))


def flatFilter(f, iter):
    return itertools.chain(filter(f, iter))


def is_ascii_alpha(word):
    # string isalpha method allows unicode, which we don't want
    return all(c >= "a" and c <= "z" for c in word)


def mapper(word, output):
    wl = word.lower()
    if is_ascii_alpha(wl) and len(wl) > 0:
        output["word"].append((wl, 1))
    for letter in wl:
        if is_ascii_alpha(letter):
            output["letter"].append((letter, 1))


def main():
    if len(sys.argv) != 6:
        raise RuntimeError(
            "Usage: mapper <input file url> <output bucket url> <chunk start byte> <chunk end byte> <ranges>\n"
            + 'where ranges is a comma-separated list of ranges, eg. "a-d,e-g,h-w,x-z" and the start/end bytes are inclusive/exclusive respectively.'
        )
    src_bucket, src_filename = s3helper.get_bucket_and_file(sys.argv[1])
    dst_bucket = s3helper.get_bucket_from_s3_url(sys.argv[2])
    print(dst_bucket)
    dst_directory = os.environ["JOB_ID"]
    chunk_range = (int(sys.argv[3]), int(sys.argv[4]))
    ranges = sys.argv[5].split(",")

    file_contents = s3helper.download_chunk(src_bucket, src_filename, chunk_range)
    output = {"word": [], "letter": []}
    for token in re_split.split(file_contents):
        mapper(token, output)

    for r in ranges:
        start, end = r.split("-")
        lrange = {chr(c) for c in range(ord(start), ord(end) + 1)}
        data = json.dumps(
            {
                "word": list(flatFilter(lambda x: x[0][0] in lrange, output["word"])),
                "letter": list(flatFilter(lambda x: x[0] in lrange, output["letter"])),
            },
            separators=[",", ":"],  # Remove whitespace
        )
        s3helper.upload_file(dst_bucket, f"{dst_directory}/{r}", data.encode())

    print(f"{len(file_contents.encode())} bytes")
    print(f'{len(output["word"])} words')
    print(f'{len(output["letter"])} letters')


if __name__ == "__main__":
    main()
