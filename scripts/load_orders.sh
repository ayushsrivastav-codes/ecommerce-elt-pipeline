#!/bin/bash

set -e

echo "===== LOAD ORDERS: CSV → HDFS ====="

ORDERS_FILE="$HOME/ecommerce-elt/input/orders.csv"
HDFS_PATH="/data/raw/orders"

if [ ! -f "$ORDERS_FILE" ]; then
    echo "ERROR: Orders file not found: $ORDERS_FILE"
    exit 1
fi

hdfs dfs -mkdir -p "$HDFS_PATH"

hdfs dfs -put -f "$ORDERS_FILE" "$HDFS_PATH/"

echo "===== ORDERS LOADED ====="
hdfs dfs -ls "$HDFS_PATH"
