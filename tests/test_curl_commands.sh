#!/bin/bash
# ============================================================================
# AeroSense API — curl test commands
# ============================================================================
BASE="http://localhost:5000"

echo "============================================"
echo " AeroSense REST API — Integration Tests"
echo " BASE URL: $BASE"
echo "============================================"

# ---- 1. Health check -------------------------------------------------------
echo -e "\n[1/6] GET /api/v1/health"
curl -s "$BASE/api/v1/health" | python3 -m json.tool

# ---- 2. List sensor types --------------------------------------------------
echo -e "\n[2/6] GET /api/v1/sensors"
curl -s "$BASE/api/v1/sensors" | python3 -m json.tool

# ---- 3. Latest reading -----------------------------------------------------
echo -e "\n[3/6] GET /api/v1/sensors/temperature/latest"
curl -s "$BASE/api/v1/sensors/temperature/latest" | python3 -m json.tool

echo -e "\n[3b] GET /api/v1/sensors/humidity/latest"
curl -s "$BASE/api/v1/sensors/humidity/latest" | python3 -m json.tool

echo -e "\n[3c] GET /api/v1/sensors/co2/latest  (expect 404)"
curl -s "$BASE/api/v1/sensors/co2/latest" | python3 -m json.tool

# ---- 4. Daily stats --------------------------------------------------------
echo -e "\n[4/6] GET /api/v1/sensors/temperature/stats?days=3"
curl -s "$BASE/api/v1/sensors/temperature/stats?days=3" | python3 -m json.tool

echo -e "\n[4b] GET /api/v1/sensors/temperature/stats?days=-1  (expect 422)"
curl -s "$BASE/api/v1/sensors/temperature/stats?days=-1" | python3 -m json.tool

echo -e "\n[4c] GET /api/v1/sensors/temperature/stats?days=abc  (expect 400)"
curl -s "$BASE/api/v1/sensors/temperature/stats?days=abc" | python3 -m json.tool

# ---- 5. Anomalies ----------------------------------------------------------
echo -e "\n[5/6] GET /api/v1/anomalies?limit=5"
curl -s "$BASE/api/v1/anomalies?limit=5" | python3 -m json.tool

echo -e "\n[5b] GET /api/v1/anomalies?sensor=temperature&limit=3"
curl -s "$BASE/api/v1/anomalies?sensor=temperature&limit=3" | python3 -m json.tool

# ---- 6. POST a reading -----------------------------------------------------
echo -e "\n[6/6] POST /api/v1/readings"
curl -s -X POST "$BASE/api/v1/readings" \
  -H "Content-Type: application/json" \
  -d '{
    "sensor": "temperature",
    "value": 23.7,
    "unit": "C",
    "timestamp": 1716163200000,
    "source": "site-F-rack-9",
    "anomaly": false
  }' | python3 -m json.tool

echo -e "\n[6b] POST /api/v1/readings (invalid sensor — expect 422)"
curl -s -X POST "$BASE/api/v1/readings" \
  -H "Content-Type: application/json" \
  -d '{
    "sensor": "co2",
    "value": 400,
    "unit": "ppm",
    "timestamp": 1716163200000,
    "source": "test",
    "anomaly": false
  }' | python3 -m json.tool

echo -e "\n[6c] POST /api/v1/readings (missing fields — expect 422)"
curl -s -X POST "$BASE/api/v1/readings" \
  -H "Content-Type: application/json" \
  -d '{"sensor": "temperature"}' | python3 -m json.tool

# ---- Error handling tests --------------------------------------------------
echo -e "\n[E1] GET /api/v1/nonexistent (expect 404)"
curl -s "$BASE/api/v1/nonexistent" | python3 -m json.tool

echo -e ""
echo "============================================"
echo " All tests completed."
echo "============================================"
