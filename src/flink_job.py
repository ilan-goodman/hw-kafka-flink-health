import json
from datetime import datetime
from typing import Any, Dict

from pyflink.datastream import (
    StreamExecutionEnvironment,
    TimeCharacteristic,
    Time,
)
from pyflink.common import Types, WatermarkStrategy, Duration
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
)
from pyflink.common.serialization import SimpleStringSchema


def parse_event(value: str) -> Dict[str, Any]:
    """
    Parse a JSON string into a dict and perform basic validation.

    This function is pure Python and will be unit-tested.
    It should:
      - parse JSON
      - ensure required fields exist: patient_id, timestamp, heart_rate_bpm
      - convert timestamp to a Python datetime or epoch millis
      - possibly clamp heart_rate_bpm to a valid range or drop invalid records

    TODO: implement parsing & validation logic.
    """
    raise NotImplementedError("parse_event() is not implemented yet")


def classify_window(avg_hr: float) -> str:
    """
    Classify a window based on its average heart rate.

    Returns one of: "tachycardia", "bradycardia", "normal".

    TODO: implement classification logic using thresholds, e.g.:
      - avg_hr > 100 -> "tachycardia"
      - avg_hr < 50  -> "bradycardia"
      - else -> "normal"
    """
    raise NotImplementedError("classify_window() is not implemented yet")


def build_env() -> StreamExecutionEnvironment:
    """
    Create and configure the StreamExecutionEnvironment.

    This is factored out for easier testing.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    env.enable_checkpointing(5000)  # 5s, can be tuned
    return env


def main():
    env = build_env()

    # Define Kafka source
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_topics("heart_rate_events")
        .set_group_id("flink-heart-monitor")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(
            lambda event, ts: int(
                # TODO: extract event-time from parsed event
                # You may want to return epoch millis here
                0
            )
        )
    )

    # Ingest raw strings
    ds = env.from_source(
        source,
        watermark_strategy,
        "heart_rate_events_source",
    )

    # Parse events
    parsed = ds.map(
        lambda s: parse_event(s),
        output_type=Types.MAP(Types.STRING(), Types.STRING()),
    )

    # TODO: You may want to filter out invalid records here, e.g.:
    # parsed = parsed.filter(lambda e: e is not None)

    # TODO: Assign timestamps/watermarks based on parsed event-time if you didn't do it earlier.

    # Key by patient_id and apply windowed aggregation.
    # For example: 1-minute tumbling or sliding windows computing avg/min/max.
    #
    # Hints:
    #   - Use key_by(...) with patient_id.
    #   - Use timeWindow or window with a Time window.
    #   - Use an aggregate or reduce function to compute avg/min/max.
    #   - Use classify_window on the aggregated result.
    #
    # The final output should be a JSON string with fields like:
    #   {
    #     "patient_id": "...",
    #     "window_start": "...",
    #     "window_end": "...",
    #     "avg_hr": 87.2,
    #     "min_hr": 60,
    #     "max_hr": 120,
    #     "alert_type": "tachycardia" / "bradycardia" / "normal"
    #   }

    # placeholder, to be replaced by student code
    alerts_stream = parsed.map(
        lambda e: json.dumps({"todo": "implement windowed aggregation"}),
        output_type=Types.STRING(),
    )

    # Kafka sink for alerts
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("heart_rate_alerts")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    alerts_stream.sink_to(sink)

    env.execute("HeartRateAlertsJob")


if __name__ == "__main__":
    main()
