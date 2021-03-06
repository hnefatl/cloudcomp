FROM openjdk:8-alpine

# Basic setup
RUN set -ex && \
    apk upgrade --no-cache && \
    apk add --no-cache bash curl tar tini libc6-compat linux-pam gcc musl-dev libffi-dev openssl-dev && \
    mkdir -p /opt/spark && \
    mkdir -p /opt/spark/work-dir && \
    touch /opt/spark/RELEASE && \
    rm /bin/sh && \
    ln -sv /bin/bash /bin/sh && \
    echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su && \
    chgrp root /etc/passwd && chmod ug+rw /etc/passwd

# SPARK
ENV SPARK_VERSION 2.4.0
ENV SPARK_PACKAGE spark-${SPARK_VERSION}-bin-hadoop2.7
ENV SPARK_HOME /usr/spark-${SPARK_VERSION}
ENV PATH $PATH:${SPARK_HOME}/bin
RUN curl -sL --retry 3 \
  "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz" \
  | gunzip \
  | tar x -C /usr/ \
 && mv /usr/$SPARK_PACKAGE $SPARK_HOME \
 && chown -R root:root $SPARK_HOME \
 && cp $SPARK_HOME/kubernetes/dockerfiles/spark/entrypoint.sh /usr/entrypoint.sh
WORKDIR $SPARK_HOME/work-dir

# Python
ENV PYTHONPATH ${SPARK_HOME}/python/lib/pyspark.zip:${SPARK_HOME}/python/lib/py4j-*.zip
RUN apk add --no-cache python3 python3-dev && \
    python3 -m ensurepip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    # We remove ensurepip since it adds no functionality since pip is
    # installed on the image and it just takes up 1.6MB on the image
    rm -r /usr/lib/python*/ensurepip && \
    pip3.6 install --upgrade pip setuptools && \
    # Removed the .cache to save space
    rm -r /root/.cache

RUN pip3.6 install pymysql tabulate

COPY wlc-spark/wordlettercount.py $SPARK_HOME/work-dir/
COPY common/*.py $SPARK_HOME/work-dir/common/
COPY wlc-spark/jars/*.jar $SPARK_HOME/jars/

ENTRYPOINT [ "/usr/entrypoint.sh" ]
