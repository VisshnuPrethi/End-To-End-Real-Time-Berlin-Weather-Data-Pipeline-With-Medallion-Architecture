#!/bin/bash

# 1. LOAD ENVIRONMENT VARIABLES (Required for Cron)
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export SPARK_HOME=/opt/spark-3.5.7-bin-hadoop3
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export PATH=$PATH:$SPARK_HOME/bin:$JAVA_HOME/bin

# --- Configuration ---
RAW_TO_SILVER="/home/adm-mcsc/spark_jobs/raw_to_silver.py"
SILVER_TO_GOLD="/home/adm-mcsc/spark_jobs/silver_to_gold.py"

echo "------------------------------------------------"
echo "🚀 Starting Weather Data Pipeline: $(date)"
echo "------------------------------------------------"

# Step 1: Run Raw to Silver (Updated with absolute path)
echo "✨ Step 1: Cleaning Raw data into Silver layer..."
$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode client $RAW_TO_SILVER

if [ $? -eq 0 ]; then
    echo "✅ Silver Layer Success!"
else
    echo "❌ Silver Layer Failed! Exiting..."
    exit 1
fi

# Step 2: Run Silver to Gold (Updated with absolute path)
echo "🏆 Step 2: Generating Gold insights..."
$SPARK_HOME/bin/spark-submit --master yarn --deploy-mode client $SILVER_TO_GOLD

if [ $? -eq 0 ]; then
    echo "✅ Gold Layer Success!"
else
    echo "❌ Gold Layer Failed!"
    exit 1
fi

echo "------------------------------------------------"
echo "🏁 Pipeline Completed Successfully at $(date)"
echo "------------------------------------------------"

# Step 3: Sync Gold to InfluxDB
echo "📊 Step 3: Pushing data to Dashboard (Worker 1)..."

# Force BOTH Driver and Worker to use the venv
export PYSPARK_PYTHON=/home/adm-mcsc/spark_jobs/venv/bin/python
export PYSPARK_DRIVER_PYTHON=/home/adm-mcsc/spark_jobs/venv/bin/python

$SPARK_HOME/bin/spark-submit \
    --master local[*] \
    /home/adm-mcsc/spark_jobs/gold_to_influx.py

if [ $? -eq 0 ]; then
    echo "✅ Dashboard Sync Success!"
else
    echo "❌ Dashboard Sync Failed!"
    exit 1
fi
