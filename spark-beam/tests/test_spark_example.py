import json
import unittest

from src.spark_example import GenerateEvent, ModelInference


class GenerateEventTest(unittest.TestCase):
    def test_generates_deterministic_events_across_multiple_streams(self):
        generator = GenerateEvent(num_events=3, events_per_batch=2)

        with self.assertLogs("src.spark_example", level="INFO") as logs:
            stream_batches = [
                list(generator.process((stream_index, batch_index)))
                for stream_index in range(2)
                for batch_index in range(2)
            ]
        events = [json.loads(event) for batch in stream_batches for event in batch]

        self.assertEqual(len(logs.output), 4)
        self.assertIn("stream 1 batch 1/2: 2 events", logs.output[0])
        self.assertIn("stream 2 batch 2/2: 1 events", logs.output[3])
        self.assertEqual(
            [event["event_id"] for event in events],
            [f"evt_{index}" for index in range(6)],
        )
        self.assertEqual([event["stream_id"] for event in events], [0, 0, 0, 1, 1, 1])
        self.assertEqual(stream_batches[0][0], list(generator.process((0, 0)))[0])

        event = events[0]
        self.assertEqual(event["type"], "click")
        self.assertGreaterEqual(event["amount"], 1)
        self.assertLessEqual(event["amount"], 1200)
        self.assertGreaterEqual(event["failed_logins"], 0)
        self.assertLessEqual(event["failed_logins"], 5)


class ModelInferenceTest(unittest.TestCase):
    def setUp(self):
        self.inference = ModelInference()
        self.inference.setup()

    def test_classifies_high_risk_and_normal_events(self):
        high_risk_event = {
            "event_id": "evt_0",
            "amount": 1200,
            "failed_logins": 5,
            "new_device": True,
        }
        normal_event = {
            "event_id": "evt_1",
            "amount": 1,
            "failed_logins": 0,
            "new_device": False,
        }

        high_risk_prediction = list(
            self.inference.process(json.dumps(high_risk_event))
        )
        normal_prediction = list(self.inference.process(json.dumps(normal_event)))

        self.assertEqual(high_risk_prediction, ["high_risk"])
        self.assertEqual(normal_prediction, ["normal"])

    def test_processes_consecutive_events_without_losing_predictions(self):
        events = [
            {
                "event_id": f"evt_{index}",
                "stream_id": index % 2,
                "amount": 1200,
                "failed_logins": 5,
                "new_device": True,
            }
            for index in range(3)
        ]

        predictions = [
            prediction
            for event in events
            for prediction in self.inference.process(json.dumps(event))
        ]

        self.assertEqual(predictions, ["high_risk"] * len(events))


if __name__ == "__main__":
    unittest.main()
