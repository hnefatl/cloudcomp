FROM docker.io/python:alpine

RUN pip install smart_open

COPY s3helper.py .
COPY wordlettercount-custom/mapper.py main.py

ENTRYPOINT ["python3", "main.py"]
