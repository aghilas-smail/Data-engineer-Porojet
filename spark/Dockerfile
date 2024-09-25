FROM bitnami/spark:3.5.0

# USER root

#RUN pip install --upgrade pip

WORKDIR /opt/bitnami/spark

# RUN pip install py4j==0.10.9.7
RUN pip install py4j

#COPY ./jars ./jars
COPY ./src/spark_sql/spark_stream.py ./spark_stream.py
COPY ./src/data.py ./src/data.py 

ENV POSTGRES_DOCKER_USER=host.docker.internal
ARG POSTGRES_PASSWORD
ENV POSTGRES_PASSWORD=$POSTGRES_PASSWORD

#CMD ["./bin/spark-submit", "--master", "local[*]", "--packages", "org.postgresql:postgresql:42.5.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"  ,"./spark_streaming.py"]