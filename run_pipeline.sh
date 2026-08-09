#!/bin/bash
# Runs the Kafka consumer and the synthetic data generator side by side.
# Paths resolve relative to this script, so the repo can live anywhere.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${PYTHON:-python3}"

echo "Starting Kafka Consumer..."
"$PYTHON" -u "$SCRIPT_DIR/consumer/kafka_to_minio.py" &
PID1=$!

echo "Starting Data Generator..."
"$PYTHON" -u "$SCRIPT_DIR/data-generator/fake_generator.py" &
PID2=$!

# When this script is exited/interrupted, kill both background processes
trap "kill $PID1 $PID2 2>/dev/null; exit" SIGINT SIGTERM EXIT

wait
