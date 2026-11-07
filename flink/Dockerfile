# Compile only the narrow Kafka metadata / partition-watermark extension.
FROM flink:1.20.3-scala_2.12-java17 AS source-build
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jdk-headless curl ca-certificates && rm -rf /var/lib/apt/lists/*
RUN curl --fail --location --retry 3 https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar --output /opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar
COPY flink/java/aiops/SourceEnvelope.java /tmp/aiops/SourceEnvelope.java
RUN mkdir -p /tmp/bridge-classes && javac --release 11 -encoding UTF-8 -cp '/opt/flink/lib/*' -d /tmp/bridge-classes /tmp/aiops/SourceEnvelope.java && jar cf /tmp/aiops-kafka-source.jar -C /tmp/bridge-classes .

# Matching Flink binary + Python package + connector line; intentionally pinned.
FROM flink:1.20.3-scala_2.12-java17
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-venv python3-pip ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -m venv /opt/pyflinkenv \
    && /opt/pyflinkenv/bin/pip install --no-cache-dir setuptools==80.9.0 apache-flink==1.20.3
COPY --from=source-build /opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar /opt/flink/lib/
COPY --from=source-build /tmp/aiops-kafka-source.jar /opt/flink/lib/
ENV PATH="/opt/pyflinkenv/bin:${PATH}" \
    PYFLINK_CLIENT_EXECUTABLE=/opt/pyflinkenv/bin/python \
    PYFLINK_PYTHON=/opt/pyflinkenv/bin/python
COPY --chown=flink:flink flink/detector.py flink/rules.py flink/aggregate.py flink/adaptive.py flink/stream_support.py wire_contract.py /opt/flink/jobs/
COPY --chown=flink:flink infra/submit_job.py /opt/flink/jobs/submit_job.py
RUN mkdir -p /opt/flink/checkpoints /opt/flink/savepoints \
    && chown -R flink:flink /opt/flink/checkpoints /opt/flink/savepoints
USER flink
