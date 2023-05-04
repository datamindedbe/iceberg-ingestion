FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.3.1-2.13-hadoop-3.3.4-v2

ENV PYSPARK_PYTHON python3
WORKDIR /opt/spark/work-dir
USER 0

# install AWS CLI
RUN apt update && apt install -y curl jq
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" &&\
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf aws

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt --no-cache-dir
COPY . .
RUN pip3 install .

ARG spark_uid=185
RUN useradd -u ${spark_uid} -m spark
USER ${spark_uid}