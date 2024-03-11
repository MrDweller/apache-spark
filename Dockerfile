FROM bitnami/spark:3.5

USER root

RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install wget -y

RUN pip install numpy
RUN pip install pandas
RUN pip install fsspec
RUN pip install adlfs

USER 1001