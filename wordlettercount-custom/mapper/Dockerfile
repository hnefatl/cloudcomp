FROM docker.io/python:alpine

RUN pip install smart_open

COPY wordlettercount-custom/mapper.py main.py
COPY s3helper.py .

ENTRYPOINT ["python3", "main.py"]
