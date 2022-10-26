FROM conda/miniconda3:latest

RUN pip install requests && \
    pip install pyspark && \
    pip install mysql-connector-python

RUN mkdir -p /usr/app/src/files
RUN mkdir -p /usr/app/src/java

RUN apt-get update
RUN apt-get install -y openjdk-8-jdk

WORKDIR /usr/app/src

COPY main.py ./
COPY schemas.py ./
COPY ./files/mysql-connector-java-8.0.22.jar /usr/app/src/java/