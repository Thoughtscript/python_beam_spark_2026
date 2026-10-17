FROM eclipse-temurin:8-jre-jammy

ARG SPARK_VERSION=2.4.8
ARG HADOOP_VERSION=2.7
ARG JACKSON_VERSION=2.14.1

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y \
        ca-certificates curl docker.io \
    && mkdir -p /opt/spark \
    && curl --fail --location --silent --show-error \
        "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
        --output /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz --strip-components=1 -C /opt/spark \
    && rm -f /opt/spark/jars/jackson-*.jar \
    && fetch_jackson() { \
        group="$1"; artifact="$2"; \
        curl --fail --location --silent --show-error \
            "https://repo.maven.apache.org/maven2/${group}/${artifact}/${JACKSON_VERSION}/${artifact}-${JACKSON_VERSION}.jar" \
            --output "/opt/spark/jars/${artifact}-${JACKSON_VERSION}.jar"; \
    } \
    && fetch_jackson com/fasterxml/jackson/core jackson-annotations \
    && fetch_jackson com/fasterxml/jackson/core jackson-core \
    && fetch_jackson com/fasterxml/jackson/core jackson-databind \
    && fetch_jackson com/fasterxml/jackson/dataformat jackson-dataformat-yaml \
    && fetch_jackson com/fasterxml/jackson/module jackson-module-jaxb-annotations \
    && fetch_jackson com/fasterxml/jackson/module jackson-module-paranamer \
    && fetch_jackson com/fasterxml/jackson/module jackson-module-scala_2.11 \
    && rm /tmp/spark.tgz \
    && apt-get purge -y --auto-remove curl \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/opt/spark

EXPOSE 8081

ENTRYPOINT ["/opt/spark/bin/spark-class"]
