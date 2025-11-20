from datetime import datetime, timezone

from src.flink_job import parse_event, classify_window


def test_parse_event_valid():
    raw = '{"patient_id":"p001","timestamp":"2025-11-19T07:15:23.123456+00:00","heart_rate_bpm":85}'
    event = parse_event(raw)
    assert event["patient_id"] == "p001"
    assert isinstance(event["heart_rate_bpm"], int)
    assert isinstance(event["event_time"], int)  # epoch millis


def test_parse_event_invalid_json():
    raw = "not-json"
    event = parse_event(raw)
    assert event is None


def test_parse_event_missing_fields():
    raw = '{"patient_id":"p001"}'
    event = parse_event(raw)
    assert event is None


def test_classify_window_tachycardia():
    assert classify_window(120.0) == "tachycardia"


def test_classify_window_bradycardia():
    assert classify_window(40.0) == "bradycardia"


def test_classify_window_normal():
    assert classify_window(75.0) == "normal"