CREATE DATABASE IF NOT EXISTS ecommerce_dw;

USE ecommerce_dw;

CREATE EXTERNAL TABLE IF NOT EXISTS orders_raw (
    order_id STRING,
    customer_id STRING,
    amount STRING,
    order_date STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/raw/orders'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS customers_raw (
    customer_id STRING,
    name STRING,
    email STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/raw/customers';
