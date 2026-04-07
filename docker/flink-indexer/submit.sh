#!/usr/bin/env bash
set -euo pipefail

until curl -sf http://flink-jobmanager:8081/overview >/dev/null; do
  echo "Waiting for Flink JobManager..."
  sleep 3
done

flink run \
  -m flink-jobmanager:8081 \
  -c com.platform.archive.flink.FlinkArchiveIndexerJob \
  /opt/app/archive-maintenance.jar \
  --kafka-brokers kafka:9092 \
  --source-topic raw-archive-events \
  --merge-events-topic merge-events \
  --hdfs-tmp-base hdfs://namenode:8020/tmp \
  --target-file-size-bytes 1048576
