FROM gettyimages/spark:2.2.1-hadoop-2.7

# export JARS location
ENV SPARK_VERSION=2.2.1\
    APP_PATH=/app

ENV SPARK_HOME=/usr/spark-${SPARK_VERSION}

RUN cd ${SPARK_HOME}/jars/ &&\
    curl -O http://central.maven.org/maven2/org/apache/kafka/kafka-clients/1.0.0/kafka-clients-1.0.0.jar &&\
    curl -O http://central.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.11/2.2.0/spark-sql-kafka-0-10_2.11-2.2.0.jar &&\
    curl -O https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar

COPY conf/ ${SPARK_HOME}/conf/

ADD . ${APP_PATH}
WORKDIR ${APP_PATH}

RUN pip install -r requirements.txt
