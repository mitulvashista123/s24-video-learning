# Container image of the Flink usage-analytics job (local runs / Flink-on-K8s alternative).
# On AWS the same flink_job.py runs on the EMR Flink cluster (see terraform/main.tf).
#   docker build -f services/usage-analytics-flink/Dockerfile -t s24/usage-analytics-flink services
FROM flink:1.18.1-scala_2.12-java11

ARG PYFLINK_VERSION=1.18.1
ARG KAFKA_CONNECTOR_VERSION=3.2.0-1.18

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip python3-dev curl \
 && ln -sf /usr/bin/python3 /usr/bin/python \
 && pip3 install --no-cache-dir "apache-flink==${PYFLINK_VERSION}" \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/flink/usrlib /opt/s24 \
 && curl -fsSL -o /opt/flink/usrlib/flink-sql-connector-kafka.jar \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"

COPY usage-analytics-flink/flink_job.py /opt/s24/flink_job.py
RUN chown -R flink:flink /opt/flink/usrlib /opt/s24

USER flink
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092

# Starts an embedded mini-cluster and runs the job (use `flink run -py` against a session cluster otherwise).
CMD ["sh", "-c", "python /opt/s24/flink_job.py --bootstrap-servers ${KAFKA_BOOTSTRAP_SERVERS}"]
