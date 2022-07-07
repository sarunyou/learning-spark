FROM docker.io/bitnami/spark:3
RUN curl https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar --output /opt/bitnami/spark/jars/gcs-connector-hadoop3-latest.jar