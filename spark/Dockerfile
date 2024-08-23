FROM ubuntu:23.10

USER root

ADD setup/spark-3.4.3-bin-hadoop3.tgz /opt

RUN apt-get update && apt-get install -y openjdk-17-jdk
RUN apt-get update && apt-get install -y python3-pip
#RUN python3 -m pip install transformers torch safetensors sentencepiece pandas pyarrow --break-system-packages
RUN python3 -m pip install numpy findspark pyspark==3.4.3 pandas transformers torch safetensors sentencepiece elasticsearch==8.13.2 --break-system-packages

RUN ln -s /opt/spark-3.4.3-bin-hadoop3 /opt/spark

#ENV PATH="${PATH}:/usr/bin/python3"
#ENV PATH="${PATH}:/opt/spark/spark-3.4.3-bin-hadoop3"
ENV SPARK_HOME=/opt/spark
ENV PATH="${PATH}:${SPARK_HOME}/bin"

COPY spark_translate.py ./

RUN ls -l ${SPARK_HOME}/bin/spark-submit

ENTRYPOINT ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.13.4", "spark_translate.py"]