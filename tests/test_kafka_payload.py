import json
from datetime import datetime, timezone

from src.kafka_producer import generate_heart_rate_event


def test_generate_heart_rate_event_fields():
    event = generate_heart_rate_event("p001")
    assert "patient_id" in event
    assert "timestamp" in event
    assert "heart_rate_bpm" in event

    assert event["patient_id"] == "p001"
    assert isinstance(event["heart_rate_bpm"], int)

    # Check timestamp is ISO8601 and parseable
    ts = datetime.fromisoformat(event["timestamp"])
    assert ts.tzinfo is not None


def test_generate_heart_rate_event_range():
    # This is deliberately loose: just ensure plausible range
    values = [generate_heart_rate_event("p001")["heart_rate_bpm"] for _ in range(100)]
    assert min(values) > 30  # probably no impossible values
    assert max(values) < 200