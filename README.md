# Cloud Computing

# Project Structure

## Task 1

The entry point for task 1 is `cli.py` in the top-level directory. This must be run from the directory it is in. `clusterconfig.py` contains the code relevant to fetching and maintaining local configuration.

### Run Instructions

After installing all dependencies, call `python cli.py`.

## Task 2

The base directory is `wlc-spark`. The entry point is `wordletterpoint.py`. This is not run locally, but instead is packaged into the clgroup8/wordlettercount Docker image, which is run on the k8s cluster.

The option to run it through the CLI is 5. The output can be viewed with option 52. The pre-requisite is that a validated k8s cluster is running. This requires 1 argument which is the input file.

The input file can have a protocol scheme of `s3://`, `s3a://` or `https://`. The caveat with `https://` URLs is that they must point to a file in an S3 bucket (our program does not accept generic `https://` URLs).

## Task 3

The base directory for this project is `wlc-custom`. The entrypoint is `deploy.py`. This does not support being run locally and a valid k8s cluster is required.

A summary of the files in the directory are:

`deploy.py` - This deploys this application onto Kubernetes. This is called by the CLI in order to start a k8s pod for the MapReduce master. This script waits until the master has terminated or succeeded before returning.

`master.py` - This contains the application logic for the MapReduce master. This is responsible for starting the mappers and reducers and writing the final output into RDS.

`mapreduce.py` - This file contains classes which are used by `master.py`.

`mapper.py` and `mapper-naive.py` - These are both implementations of a MapReduce mapper for WordLetterCount. `mapper-naive.py` corresponds exactly to the idea of MapReduce. However, we noticed a huge explosion in the size of the output of a strict MapReduce mapper as every letter in the input produces a tuple as the output. Thus we wrote `mapper.py`, which does not directly output `(word, 1)` and `(letter, 1)` for each word and letter in input, but does some pre-processing first.

`reducer.py` - This is an implementation of a MapReduce reducer for WordLetterCount.

This does not support being run locally, and must be run through the CLI using option 6. The output can be viewed with option 62. The required arguments are the input file (which has the same restrictions as the Task 2 input) and the chunk size (a default of 25MB is used). A valid k8s cluster must be running.

## Common Dependencies

Our project is written in Python and Bash. Since there were a few common dependencies between different parts of the project, these can be found in the `common/` folder, which is a Python module.

## Utility Scripts

`utils/` contains some utility scripts which may help in the development of the project.



## Dependencies

### Python
- boto3
- pymysql
- tabulate

### External
- kops
- kubectl
- aws_cli

