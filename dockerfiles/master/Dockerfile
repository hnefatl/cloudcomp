FROM docker.io/python:alpine

RUN apk upgrade --no-cache && \
    apk add --no-cache build-base python3-dev libffi-dev openssl-dev && \
    pip install kubernetes pymysql tabulate boto3 smart_open

COPY wlc-custom/master.py wlc-custom/mapreduce.py ./
COPY common/*.py common/

ENTRYPOINT ["python3", "master.py"]
