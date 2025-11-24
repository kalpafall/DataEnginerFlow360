#!/bin/bash

if [ "$SPARK_MODE" = "master" ]; then
    echo "Starting Spark Master..."
    exec ${SPARK_HOME}/sbin/start-master.sh && tail -f ${SPARK_HOME}/logs/*
elif [ "$SPARK_MODE" = "worker" ]; then
    echo "Starting Spark Worker..."
    exec ${SPARK_HOME}/sbin/start-worker.sh ${SPARK_MASTER_URL} && tail -f ${SPARK_HOME}/logs/*
else
    exec "$@"
fi
