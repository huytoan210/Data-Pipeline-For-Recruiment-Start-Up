FROM bitnami/spark:3.3.2

USER root

# Thêm Spark Cassandra Connector
COPY jars/spark-cassandra-connector-assembly_2.12-3.3.0.jar /opt/bitnami/spark/jars/
# Thêm thư viện MySQL Connector
COPY jars/mysql-connector-j-8.0.33.jar /opt/bitnami/spark/jars/

# Cài thư viện Python
RUN pip install pyspark cassandra-driver mysql-connector-python time-uuid

WORKDIR /app
COPY app/ /app
