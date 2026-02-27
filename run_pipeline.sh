#!/bin/bash
echo "Starting Kafka Consumer..."
/usr/local/bin/python3 -u /Users/antarangsharma/banking-modern-datastack/consumer/kafka_to_minio.py &
PID1=$!

echo "Starting Data Generator..."
/usr/local/bin/python3 -u /Users/antarangsharma/banking-modern-datastack/data-generator/fake_generator.py &
PID2=$!

# When this script is exited/interrupted, kill both background processes
trap "kill $PID1 $PID2; exit" SIGINT SIGTERM EXIT

wait
