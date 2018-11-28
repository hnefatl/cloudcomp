FROM docker.io/python:alpine

RUN pip install smart_open

COPY wlc-custom/mapper.py .
COPY common/*.py common/

ENTRYPOINT ["python3", "mapper.py"]
