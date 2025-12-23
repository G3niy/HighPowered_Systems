FROM spark:python3

USER root

RUN pip install cassandra-driver

RUN curl -L -o /opt/spark/jars/spark-cassandra-connector-assembly_2.13-3.5.1.jar \
    https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.13/3.5.1/spark-cassandra-connector-assembly_2.13-3.5.1.jar

RUN pip install pyspark[sql]

COPY jars/ /opt/spark/jars/

RUN ls -la /opt/spark/jars/ | grep cassandra