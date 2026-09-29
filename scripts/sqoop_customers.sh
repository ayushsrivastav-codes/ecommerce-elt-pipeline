#!/bin/bash

set -e

# Load private credentials
source ~/.ecommerce_elt_env

echo "===== SQOOP: MYSQL → HDFS ====="

hdfs dfs -rm -r -f /data/raw/customers

sqoop import \
  --connect jdbc:mysql://localhost:3306/ecommerce \
  --username sqoop_user \
  --password "$SQOOP_PASSWORD" \
  --table customers \
  --target-dir /data/raw/customers \
  --fields-terminated-by ',' \
  --lines-terminated-by '\n' \
  --m 1

echo "===== CUSTOMERS LOADED ====="
hdfs dfs -ls /data/raw/customers
