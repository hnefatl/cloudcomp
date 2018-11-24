FROM docker.io/python:alpine

RUN pip install smart_open

COPY s3helper.py .
COPY reducer.py main.py

ENTRYPOINT ["python3", "main.py"]

