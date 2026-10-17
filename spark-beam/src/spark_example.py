import json
import logging
import socket
import threading
import time

import apache_beam as beam
from apache_beam.metrics import Metrics, MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.runner import PipelineState

NUM_STREAMS = 10
EVENTS_PER_STREAM = 1_000_000
NUM_EVENTS = NUM_STREAMS * EVENTS_PER_STREAM
EVENTS_PER_BATCH = 1_000
LOG_PROGRESS_EVERY = 100_000
PROGRESS_INTERVAL_SECONDS = 10
EVENT_TYPES = ("click", "view", "purchase", "login")
EVENT_SCHEMA = {
    "event_id": "string",
    "stream_id": "integer",
    "user_id": "string",
    "type": "click | view | purchase | login",
    "amount": "float (1-1200)",
    "failed_logins": "integer (0-5)",
    "new_device": "boolean",
}
JOB_SERVER_HOST = "127.0.0.1"
JOB_SERVER_PORT = 8099
JOB_SERVER_TIMEOUT_SECONDS = 120
METRICS_NAMESPACE = "fake-risk-model"
LOGGER = logging.getLogger(__name__)


class GenerateEvent(beam.DoFn):
    def __init__(self, num_events=NUM_EVENTS, events_per_batch=EVENTS_PER_BATCH):
        self.event_types = EVENT_TYPES
        self.log_progress_every = LOG_PROGRESS_EVERY
        self.num_events = num_events
        self.events_per_batch = events_per_batch
        self.generated_events = Metrics.counter(
            METRICS_NAMESPACE, "events_generated"
        )

    def process(self, stream_batch):
        import json
        import logging

        stream_index, batch_index = stream_batch
        first_index = batch_index * self.events_per_batch
        batch_end = min(first_index + self.events_per_batch, self.num_events)
        batch_count = (
            self.num_events + self.events_per_batch - 1
        ) // self.events_per_batch
        logger = logging.getLogger(__name__)
        logger.info(
            "Generating stream %s batch %s/%s: %s events (evt_%s through "
            "evt_%s).",
            f"{stream_index + 1:,}",
            f"{batch_index + 1:,}",
            f"{batch_count:,}",
            f"{batch_end - first_index:,}",
            f"{stream_index * self.num_events + first_index:,}",
            f"{stream_index * self.num_events + batch_end - 1:,}",
        )
        for index in range(first_index, batch_end):
            event_index = stream_index * self.num_events + index
            event = {
                "event_id": f"evt_{event_index}",
                "stream_id": stream_index,
                "user_id": f"user_{(event_index * 73 + 19) % 500 + 1}",
                "type": self.event_types[
                    (event_index * 3 + event_index // 7) % len(self.event_types)
                ],
                "amount": round(1 + (event_index * 7919 % 119_900) / 100, 2),
                "failed_logins": (event_index * 13 + event_index // 11) % 6,
                "new_device": (event_index * 17 + 5) % 2 == 0,
            }
            self.generated_events.inc()
            if (index + 1) % self.log_progress_every == 0:
                logger.info(
                    "Event generation progress: stream %s created %s of "
                    "%s events.",
                    f"{stream_index + 1:,}",
                    f"{index + 1:,}",
                    f"{self.num_events:,}",
                )
            yield json.dumps(event)


class ModelInference(beam.DoFn):
    def __init__(self):
        self.log_progress_every = LOG_PROGRESS_EVERY
        self.num_events = NUM_EVENTS
        self.processed_events = Metrics.counter(
            METRICS_NAMESPACE, "events_processed"
        )
        self.high_risk_events = Metrics.counter(
            METRICS_NAMESPACE, "high_risk_events"
        )
        self.normal_events = Metrics.counter(METRICS_NAMESPACE, "normal_events")

    def setup(self):
        self.model_weights = (0.55, 0.3, 0.15)
        self.risk_threshold = 0.58

    def process(self, raw_event):
        import json
        import logging

        event = json.loads(raw_event)
        score = min(
            1.0,
            event["amount"] / 1200 * self.model_weights[0]
            + event["failed_logins"] / 5 * self.model_weights[1]
            + (self.model_weights[2] if event["new_device"] else 0),
        )
        prediction = (
            "high_risk" if score >= self.risk_threshold else "normal"
        )
        self.processed_events.inc()
        if prediction == "high_risk":
            self.high_risk_events.inc()
        else:
            self.normal_events.inc()
        event_index = int(event["event_id"].removeprefix("evt_"))
        if event_index % self.log_progress_every == 0:
            logging.getLogger(__name__).info(
                "Model inference progress: processed event %s of %s "
                "(prediction: %s).",
                f"{event_index + 1:,}",
                f"{self.num_events:,}",
                prediction,
            )
        yield prediction


def get_metric_counts(result):
    counters = result.metrics().query(
        MetricsFilter().with_namespace(METRICS_NAMESPACE)
    )["counters"]
    return {
        counter.key.metric.name: (
            counter.committed
            if counter.committed is not None
            else counter.attempted
        )
        for counter in counters
    }


def wait_for_job_server():
    LOGGER.info(
        "Waiting for Beam Spark Job Server at %s:%s.",
        JOB_SERVER_HOST,
        JOB_SERVER_PORT,
    )
    deadline = time.monotonic() + JOB_SERVER_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(
                (JOB_SERVER_HOST, JOB_SERVER_PORT), timeout=1
            ):
                LOGGER.info("Beam Spark Job Server is ready.")
                return
        except OSError:
            time.sleep(1)

    raise TimeoutError(
        f"Beam Spark Job Server at {JOB_SERVER_HOST}:{JOB_SERVER_PORT} "
        f"did not become available within {JOB_SERVER_TIMEOUT_SECONDS} seconds."
    )


def run_pipeline():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    LOGGER.info("Starting the Beam pipeline for %s fake events.", f"{NUM_EVENTS:,}")
    LOGGER.info("Fake event schema: %s", json.dumps(EVENT_SCHEMA, sort_keys=True))
    wait_for_job_server()
    options = PipelineOptions(
        [
            "--runner=PortableRunner",
            f"--job_endpoint={JOB_SERVER_HOST}:{JOB_SERVER_PORT}",
            "--environment_type=DOCKER",
            "--environment_config=apache/beam_python3.10_sdk:2.45.0",
            "--default_sdk_harness_log_level=INFO",
        ]
    )

    started_at = time.monotonic()
    LOGGER.info(
        "Pipeline steps: create %s event streams with batches of up to %s "
        "events each; continuously generate events, infer risk predictions, "
        "and count prediction labels.",
        f"{NUM_STREAMS:,}",
        f"{EVENTS_PER_BATCH:,}",
    )
    LOGGER.info(
        "The bounded demonstration streams %s events through inference "
        "across all %s streams.",
        f"{NUM_EVENTS:,}",
        f"{NUM_STREAMS:,}",
    )
    pipeline = beam.Pipeline(options=options)
    predictions = (
        pipeline
        | "Create batches for each event stream"
        >> beam.Create(
            [
                (stream_index, batch_index)
                for stream_index in range(NUM_STREAMS)
                for batch_index in range(
                    (EVENTS_PER_STREAM + EVENTS_PER_BATCH - 1)
                    // EVENTS_PER_BATCH
                )
            ]
        )
        | "Generate fake events"
        >> beam.ParDo(GenerateEvent(num_events=EVENTS_PER_STREAM))
        | "Run fake risk model" >> beam.ParDo(ModelInference())
    )
    predictions | "Count prediction labels" >> beam.combiners.Count.PerElement()

    completion = threading.Event()
    wait_errors = []

    def execute_pipeline():
        try:
            with pipeline:
                pass
        except Exception as error:
            wait_errors.append(error)
        finally:
            completion.set()

    run_thread = threading.Thread(target=execute_pipeline, daemon=True)
    run_thread.start()
    while not completion.wait(PROGRESS_INTERVAL_SECONDS):
        LOGGER.info(
            "Pipeline is still running (elapsed: %.1fs). Spark is generating "
            "events, running the risk model, and counting prediction labels.",
            time.monotonic() - started_at,
        )

    run_thread.join()
    if wait_errors:
        raise wait_errors[0]

    if pipeline.result is None:
        raise RuntimeError("Beam pipeline completed without a PipelineResult.")

    if pipeline.result.state != PipelineState.DONE:
        raise RuntimeError(
            "Beam pipeline finished in unexpected state: "
            f"{pipeline.result.state}."
        )

    counts = get_metric_counts(pipeline.result)
    processed = counts.get("events_processed", 0)
    generated = counts.get("events_generated", 0)
    high_risk = counts.get("high_risk_events", 0)
    normal = counts.get("normal_events", 0)
    predictions_counted = high_risk + normal
    if generated != NUM_EVENTS or processed != NUM_EVENTS:
        raise RuntimeError(
            f"Pipeline generated {generated} events and inferred "
            f"{processed}; expected {NUM_EVENTS} for each."
        )
    if predictions_counted != processed:
        raise RuntimeError(
            f"Prediction labels total {predictions_counted}; "
            f"expected {processed} labels."
        )

    LOGGER.info(
        "Pipeline completed successfully in %.1f seconds. "
        "Generated: %s; inferred: %s; high-risk: %s; normal: %s.",
        time.monotonic() - started_at,
        f"{generated:,}",
        f"{processed:,}",
        f"{high_risk:,}",
        f"{normal:,}",
    )


if __name__ == "__main__":
    run_pipeline()
