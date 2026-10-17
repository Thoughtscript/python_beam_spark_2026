# python_beam_spark_2026

[![](https://img.shields.io/badge/Apache%20Beam-2.45.0-orange.svg)](https://github.com/apache/beam/releases/tag/v2.45.0) [![](https://img.shields.io/badge/Apache%20Spark-2.4.8-E25A1C.svg)](https://archive.apache.org/dist/spark/spark-2.4.8/) [![](https://img.shields.io/badge/Python-3.10-yellow.svg)](https://hub.docker.com/layers/apache/beam_python3.10_sdk/2.45.0/images/) [![](https://img.shields.io/badge/Java-8-blue.svg)](https://hub.docker.com/_/eclipse-temurin)

Experiments with continuous event streaming with uninterruptible Model Inferencing using Python, Apache Beam, and Spark.

> AI first development demo to learn about the GCP Machine Learning Pro.

## GCP PMLE

This is a small but important sub-topic (a technique that comes up in the test material).

## Summary

## Docker Compose Services

Gotcha's:
1. Apache Beam with Spark is awkward to configure within a single Docker container (and not realistic).
1. Google's AI argued with me for a while about whether the full Spark `.jars` are really present through Python (I knew they weren't) and GitHub CoPilot confirmed my suspicions:
    ```
    ... the worker can’t launch an executor because the Job Server image contains Spark’s jars without a full Spark distribution. I’ll add the Spark runtime files executors need and verify a job can launch.
    ```
1. `SPARK_VERSION` is a bit of an issue. The AI configured this incorrectly then corrected itself:
    ```
    The worker failure confirms Spark needs a full SPARK_HOME, not just the Job Server’s .jar. I’ll unpack the matching Spark 2.4.8 distribution in each Spark service image and use its standard launcher so worker-launched executors can find Spark’s build files.
    ```
1. Keep the versions aligned: `Beam 2.45.0` (including its Python SDK harness), `Spark 2.4.8`, and `Java 8`. Spark's `Hadoop 2.7` distribution and `Jackson 2.14.1` JARs are used by both Spark Images and are seemingly required for correct serialization.
    * In particular, ensure that the various Spark version align (here, are identical) since Java conflicts can creep up.
    * This was the most time-consuming issues encountered by AI.
1. The Spark cluster will create multiple temporary Containers during processing.
    * These are expected and are Beam-created temporary Harness Containers.
1. After some hand holding I got AI to correctly configure the Spark Cluster itself:
    * It originally insisted on as many as 6 distinct Containers.
    * I used these helpful references as supplemental context to increase local session accuracy:
        * https://github.com/apache/beam/blob/master/sdks/python/apache_beam/io/hdfs_integration_test/docker-compose.yml
        * https://jaehyeon.me/blog/2024-03-28-beam-local-dev-1/
        * https://beam.apache.org/documentation/runtime/environments/
        * https://github.com/cometta/python-apache-beam-spark/blob/master/deployment.yaml

> [Screenshot](./Screenshot%202026-10-16%20at%209.23.02 PM.png) is included since the initialization and execution of all pipeline steps is usefully detailed and time-consuming to reproduce.
 
## Resources and Links

1. https://beam.apache.org/releases/pydoc/2.45.0/apache_beam.ml.inference.base.html
1. https://beam.apache.org/releases/pydoc/2.45.0/apache_beam.transforms.core.html
1. https://hub.docker.com/layers/apache/beam_python3.10_sdk/2.45.0/images/
1. https://archive.apache.org/dist/spark/spark-2.4.8/
