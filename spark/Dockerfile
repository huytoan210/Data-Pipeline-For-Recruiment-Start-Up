FROM bitnami/spark:3.3.2

USER root

COPY jars/spark-cassandra-connector-assembly_2.12-3.3.0.jar /opt/bitnami/spark/jars/

COPY jars/mysql-connector-j-8.0.33.jar /opt/bitnami/spark/jars/


RUN pip install pyspark cassandra-driver mysql-connector-python time-uuid

WORKDIR /app
COPY app/ /app
