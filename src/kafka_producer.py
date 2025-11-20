import json
import random
import time
from datetime import datetime, timezone
from typing import Dict

from kafka import KafkaProducer


def generate_heart_rate_event(patient_id: str) -> Dict:
    """
    Generate a synthetic heart rate event for a single patient.

    You may modify this function to add more complex behavior, such as:
    - temporary abnormal episodes (very high or very low heart rate)
    - different baselines for different patients
    """
    # Simple baseline model: 60-90 bpm, with occasional spikes or drops
    baseline = random.randint(65, 80)
    noise = random.randint(-10, 15)
    hr = baseline + noise

    event = {
        "patient_id": patient_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "heart_rate_bpm": hr,
    }
    return event


def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    patient_ids = [f"p{i:03d}" for i in range(1, 6)]  # 5 patients

    print("Starting heart rate event generation. Press Ctrl+C to stop.")
    try:
        while True:
            patient_id = random.choice(patient_ids)
            event = generate_heart_rate_event(patient_id)
            producer.send("heart_rate_events", value=event)
            # flush occasionally or rely on background flush
            time.sleep(0.1)  # ~10 events/sec across patients
    except KeyboardInterrupt:
        print("Stopping producer.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()