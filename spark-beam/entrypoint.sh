#!/bin/sh
set -eu

python3 -m unittest discover -s /app/tests -v

java -cp "/opt/apache/beam/jars/*" org.apache.beam.runners.spark.SparkJobServerDriver "$@" &
job_server_pid=$!
python3 /app/src/spark_example.py &
client_pid=$!

stop_processes() {
    kill "$client_pid" 2>/dev/null || true
    kill "$job_server_pid" 2>/dev/null || true
    wait "$client_pid" 2>/dev/null || true
    wait "$job_server_pid" 2>/dev/null || true
}

trap stop_processes EXIT INT TERM
wait "$client_pid"
