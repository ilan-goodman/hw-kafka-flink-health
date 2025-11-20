import json
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("Sending 10 hello-world messages to topic 'hello_input'...")
    for i in range(10):
        event = {
            "id": i,
            "message": "hello flink",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        producer.send("hello_input", value=event)
        print(f"Sent: {event}")
        time.sleep(0.2)

    producer.flush()
    producer.close()
    print("Done. You can now run the Flink job and consume from 'hello_output'.")


if __name__ == "__main__":
    main()