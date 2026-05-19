#!/usr/bin/env python3
"""
AeroSense IoT Sensor Event Producer.

Generates realistic sensor readings (temperature, humidity, pressure)
and publishes them to the sensor-events Kafka topic with key-based
partitioning by sensor type.
"""

import argparse
import json
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

# Force UTF-8 on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from kafka import KafkaProducer
from kafka.errors import KafkaError

# ---------------------------------------------------------------------------
# Configuration constants — no hard-coded absolute paths
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094"
TOPIC = "sensor-events"

# Realistic sensor ranges (from lab sessions)
SENSOR_CONFIG = {
    "temperature": {"unit": "C",   "min": 15.0, "max": 45.0,  "anom_max": 35.0},
    "humidity":    {"unit": "%",   "min": 30.0, "max": 95.0,  "anom_max": 90.0},
    "pressure":    {"unit": "hPa", "min": 980.0,"max": 1040.0,"anom_lo": 990.0, "anom_hi": 1030.0},
}

ANOMALY_RATE = 0.12          # 12% of messages are anomalies
SITES = [
    "site-A-rack-12", "site-A-rack-5", "site-B-rack-3",
    "site-B-rack-7", "site-C-rack-1", "site-D-rack-9",
    "site-E-rack-2", "site-F-rack-4", "site-G-rack-6",
    "site-H-rack-8", "site-I-rack-10", "site-J-rack-11",
]

running = True


def _signal_handler(sig, _frame):
    global running
    running = False


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _random_sensor_type() -> str:
    return random.choice(list(SENSOR_CONFIG.keys()))


def _generate_value(sensor_type: str, force_anomaly: bool) -> tuple[float, bool]:
    """Generate a value for the given sensor type.

    If *force_anomaly* is True the value is pushed outside the normal
    operating window so that downstream anomaly detection triggers.
    """
    cfg = SENSOR_CONFIG[sensor_type]
    if not force_anomaly:
        return round(random.uniform(cfg["min"], cfg["max"]), 2), False

    # Produce an out-of-threshold value
    if sensor_type == "temperature":
        val = round(random.uniform(35.1, 55.0), 2)
    elif sensor_type == "humidity":
        val = round(random.uniform(90.1, 99.9), 2)
    else:  # pressure
        if random.random() < 0.5:
            val = round(random.uniform(900.0, 989.9), 2)
        else:
            val = round(random.uniform(1030.1, 1080.0), 2)
    return val, True


def _build_message(sensor_type: str, value: float, source: str, anomaly: bool) -> dict:
    return {
        "sensor": sensor_type,
        "value": value,
        "unit": SENSOR_CONFIG[sensor_type]["unit"],
        "timestamp": int(time.time() * 1000),
        "source": source,
        "anomaly": anomaly,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="AeroSense IoT Producer")
    parser.add_argument("--count", type=int, default=100, help="Number of events to produce")
    parser.add_argument("--rate", type=int, default=10, help="Events per second (max)")
    parser.add_argument("--source", type=str, default="site-A-rack-12", help="Source identifier")
    parser.add_argument("--bootstrap-servers", type=str, default=BOOTSTRAP_SERVERS)
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers.split(","),
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
        linger_ms=5,
        batch_size=16384,
        compression_type="gzip",
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    produced = 0
    delay = 1.0 / args.rate if args.rate > 0 else 0.0

    print(f"Producer started -> {args.count} events @ ~{args.rate}/s, source={args.source}")
    print(f"Bootstrap servers: {args.bootstrap_servers}")
    print("-" * 60)

    try:
        while running and produced < args.count:
            sensor_type = _random_sensor_type()
            force_anomaly = random.random() < ANOMALY_RATE
            value, is_anomaly = _generate_value(sensor_type, force_anomaly)
            msg = _build_message(sensor_type, value, args.source, is_anomaly)

            future = producer.send(TOPIC, key=sensor_type, value=msg)
            try:
                record_meta = future.get(timeout=10)
                produced += 1
                anom_flag = "[A]" if is_anomaly else "   "
                print(
                    f"[{produced:>5}/{args.count}] {anom_flag} "
                    f"partition={record_meta.partition} offset={record_meta.offset:>6} "
                    f"key={sensor_type:<12} value={value:>8.2f} {msg['unit']}"
                )
            except KafkaError as exc:
                print(f"Delivery failed: {exc}", file=sys.stderr)

            time.sleep(delay)

    except KeyboardInterrupt:
        print("\nInterrupted by user", file=sys.stderr)
    finally:
        print("Flushing producer buffer ...")
        producer.flush(timeout=30)
        producer.close(timeout=10)
        print(f"Done. {produced} messages sent.")


if __name__ == "__main__":
    main()
