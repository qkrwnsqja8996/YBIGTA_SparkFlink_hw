#!/bin/bash
set -e

SPARK_HOME=${SPARK_HOME:-/opt/spark}

case "$SPARK_MODE" in
  master)
    exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.master.Master \
      --host "${HOSTNAME:-spark-master}" \
      --port 7077 \
      --webui-port 8080
    ;;
  worker)
    MASTER_URL="${SPARK_MASTER_URL:-spark://spark-master:7077}"
    ARGS=""
    [[ -n "${SPARK_WORKER_MEMORY:-}" ]] && ARGS="$ARGS --memory $SPARK_WORKER_MEMORY"
    [[ -n "${SPARK_WORKER_CORES:-}"  ]] && ARGS="$ARGS --cores $SPARK_WORKER_CORES"
    exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.worker.Worker \
      $ARGS "$MASTER_URL"
    ;;
  *)
    exec "$@"
    ;;
esac
