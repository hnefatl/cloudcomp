FROM docker.io/python:alpine

RUN pip install smart_open

COPY wlc-custom/reducer.py .
COPY common/*.py common/

ENTRYPOINT ["python3", "reducer.py"]

