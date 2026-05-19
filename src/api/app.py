#!/usr/bin/env python3
"""
AeroSense REST API.

Exposes sensor data from Kafka and the data lake via REST endpoints.
"""

import json
import os
import sys
import time
import traceback

from flask import Flask, jsonify, request

# Ensure the api package is importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from kafka_utils import ALLOWED_SENSORS, ALLOWED_UNITS, latest_reading, publish_reading
from lake_utils import daily_stats, recent_anomalies

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _json_response(data, status=200):
    return jsonify({"status": "success", "data": data}), status


def _error_response(message, code, error_type=None):
    payload = {
        "status": "error",
        "error": {"code": code, "type": error_type or "generic_error", "message": message},
    }
    return jsonify(payload), code


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.route("/api/v1/health", methods=["GET"])
def health():
    return _json_response({
        "service": "AeroSense IoT Data Platform API",
        "version": "1.0.0",
        "timestamp": int(time.time() * 1000),
        "healthy": True,
    })


@app.route("/api/v1/sensors", methods=["GET"])
def sensors():
    return _json_response({
        "sensor_types": sorted(ALLOWED_SENSORS),
        "count": len(ALLOWED_SENSORS),
    })


@app.route("/api/v1/sensors/<sensor_type>/latest", methods=["GET"])
def sensor_latest(sensor_type: str):
    if sensor_type not in ALLOWED_SENSORS:
        return _error_response(
            f"Unknown sensor type '{sensor_type}'. Allowed: {sorted(ALLOWED_SENSORS)}",
            404, "not_found",
        )

    reading = latest_reading(sensor_type)
    if reading is None:
        return _error_response(
            f"No recent reading found for sensor type '{sensor_type}'.",
            404, "not_found",
        )

    return _json_response(reading)


@app.route("/api/v1/sensors/<sensor_type>/stats", methods=["GET"])
def sensor_stats(sensor_type: str):
    if sensor_type not in ALLOWED_SENSORS:
        return _error_response(
            f"Unknown sensor type '{sensor_type}'. Allowed: {sorted(ALLOWED_SENSORS)}",
            404, "not_found",
        )

    days_str = request.args.get("days", "7")
    try:
        days = int(days_str)
    except ValueError:
        return _error_response(
            f"Parameter 'days' must be an integer, got '{days_str}'.",
            400, "bad_request",
        )

    if days < 1 or days > 90:
        return _error_response(
            f"Parameter 'days' must be between 1 and 90, got {days}.",
            422, "unprocessable_entity",
        )

    stats = daily_stats(sensor_type, days)
    if stats is None or stats.rdd.isEmpty():
        return _error_response(
            f"No stats available for sensor type '{sensor_type}'.",
            404, "not_found",
        )

    rows = [row.asDict() for row in stats.collect()]
    return _json_response({"sensor_type": sensor_type, "days": days, "daily_stats": rows})


@app.route("/api/v1/anomalies", methods=["GET"])
def anomalies():
    sensor_type = request.args.get("sensor")
    limit_str = request.args.get("limit", "20")

    try:
        limit = int(limit_str)
    except ValueError:
        return _error_response(
            f"Parameter 'limit' must be an integer, got '{limit_str}'.",
            400, "bad_request",
        )

    if limit < 1 or limit > 100:
        return _error_response(
            f"Parameter 'limit' must be between 1 and 100, got {limit}.",
            422, "unprocessable_entity",
        )

    if sensor_type and sensor_type not in ALLOWED_SENSORS:
        return _error_response(
            f"Unknown sensor type '{sensor_type}'. Allowed: {sorted(ALLOWED_SENSORS)}",
            422, "unprocessable_entity",
        )

    result = recent_anomalies(sensor_type, limit)
    if result is None or result.rdd.isEmpty():
        return _json_response({"anomalies": [], "count": 0})

    rows = [row.asDict() for row in result.collect()]
    return _json_response({"anomalies": rows, "count": len(rows)})


@app.route("/api/v1/readings", methods=["POST"])
def post_reading():
    body = request.get_json(silent=True)
    if body is None:
        return _error_response("Request body must be valid JSON.", 400, "bad_request")

    # Validate required fields
    required = {"sensor", "value", "unit", "timestamp", "source"}
    missing = required - set(body.keys())
    if missing:
        return _error_response(f"Missing required fields: {sorted(missing)}", 422, "unprocessable_entity")

    sensor = body["sensor"]
    if sensor not in ALLOWED_SENSORS:
        return _error_response(
            f"Invalid sensor type '{sensor}'. Allowed: {sorted(ALLOWED_SENSORS)}",
            422, "unprocessable_entity",
        )

    try:
        value = float(body["value"])
    except (TypeError, ValueError):
        return _error_response(
            f"Field 'value' must be numeric, got '{body.get('value')}'.",
            422, "unprocessable_entity",
        )

    expected_unit = ALLOWED_UNITS.get(sensor)
    if body.get("unit") != expected_unit:
        return _error_response(
            f"Invalid unit '{body.get('unit')}' for sensor '{sensor}'. Expected '{expected_unit}'.",
            422, "unprocessable_entity",
        )

    if not isinstance(body.get("source"), str) or not body["source"].strip():
        return _error_response("Field 'source' must be a non-empty string.", 422, "unprocessable_entity")

    if not isinstance(body.get("timestamp"), (int, float)):
        return _error_response("Field 'timestamp' must be a numeric epoch-ms value.", 422, "unprocessable_entity")

    # Build the message
    payload = {
        "sensor": sensor,
        "value": value,
        "unit": expected_unit,
        "timestamp": int(body["timestamp"]),
        "source": body["source"].strip(),
        "anomaly": bool(body.get("anomaly", False)),
    }

    success = publish_reading(payload)
    if not success:
        return _error_response("Failed to publish reading to Kafka.", 500, "kafka_error")

    app.logger.info("Published reading: sensor=%s value=%.2f", sensor, value)
    return _json_response({"published": payload}, 201)


# ---------------------------------------------------------------------------
# Global error handlers
# ---------------------------------------------------------------------------
@app.errorhandler(404)
def not_found(_e):
    return _error_response("The requested resource was not found.", 404, "not_found")


@app.errorhandler(405)
def method_not_allowed(_e):
    return _error_response("HTTP method not allowed for this endpoint.", 405, "method_not_allowed")


@app.errorhandler(500)
def internal_error(_e):
    traceback.print_exc()
    return _error_response("Internal server error.", 500, "internal_error")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
