"""
Kafka utility helpers for the REST API layer.
"""

import json
import os

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import KafkaError

KAFKA_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP", "localhost:9092,localhost:9093,localhost:9094"
).split(",")
TOPIC = "sensor-events"

ALLOWED_SENSORS = {"temperature", "humidity", "pressure"}
ALLOWED_UNITS = {"temperature": "C", "humidity": "%", "pressure": "hPa"}


def latest_reading(sensor_type: str) -> dict | None:
    """Fetch the most recent message for the given sensor_type from Kafka."""
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=5000,
    )
    try:
        partitions = consumer.partitions_for_topic(TOPIC)
        if not partitions:
            return None
        tps = [TopicPartition(TOPIC, p) for p in partitions]
        consumer.assign(tps)

        latest = None
        latest_ts = 0
        for tp in tps:
            end_off = consumer.end_offsets([tp])[tp]
            if end_off > 0:
                consumer.seek(tp, max(0, end_off - 10))  # scan last 10 msgs per partition
                for msg in consumer:
                    if msg.key == sensor_type and msg.value:
                        if msg.value.get("timestamp", 0) > latest_ts:
                            latest_ts = msg.value["timestamp"]
                            latest = msg.value
        return latest
    finally:
        consumer.close()


def publish_reading(payload: dict) -> bool:
    """Publish a reading to the sensor-events topic."""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks="all",
        retries=3,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    try:
        future = producer.send(TOPIC, key=payload["sensor"], value=payload)
        future.get(timeout=10)
        return True
    except KafkaError:
        return False
    finally:
        producer.close()
