#!/bin/bash
# Script to run the PySpark streaming job
# with configuration for Kafka and Delta Lake

#param: script to run
SCRIPT=${1:-streaming_customer_orders.py}

echo "Starting PySpark streaming job $SCRIPT..."
echo "=================================================="

/opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.1 \
  --conf spark.sql.streaming.checkpointLocation=/spark-checkpoints \
  /app/$SCRIPT

