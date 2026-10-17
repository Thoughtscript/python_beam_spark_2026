FROM python:3.10-slim-bullseye AS beam-python

WORKDIR /app
COPY spark-beam/requirements.txt ./requirements.txt
RUN python -m pip install --no-cache-dir -r requirements.txt

FROM apache/beam_spark_job_server:2.45.0

USER root
COPY --from=beam-python /usr/local /usr/local
WORKDIR /app
COPY spark-beam/src/spark_example.py ./src/spark_example.py
COPY spark-beam/tests ./tests
COPY spark-beam/entrypoint.sh /usr/local/bin/spark-beam-entrypoint

RUN chmod +x /usr/local/bin/spark-beam-entrypoint

EXPOSE 8097 8098 8099

ENTRYPOINT ["/usr/local/bin/spark-beam-entrypoint"]
CMD ["--spark-master-url=spark://spark-master:7077", "--job-host=spark-beam"]
