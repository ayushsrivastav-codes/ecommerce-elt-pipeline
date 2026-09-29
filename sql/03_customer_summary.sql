USE ecommerce_dw;

SET hive.auto.convert.join=false;

DROP TABLE IF EXISTS customer_order_summary;

CREATE TABLE customer_order_summary (
    customer_id STRING,
    name STRING,
    total_spent DOUBLE,
    total_orders INT
)
STORED AS PARQUET;

INSERT OVERWRITE TABLE customer_order_summary
SELECT
    c.customer_id,
    c.name,
    SUM(o.amount),
    COUNT(o.order_id)
FROM orders_transformed o
JOIN customers_raw c
    ON o.customer_id = c.customer_id
GROUP BY
    c.customer_id,
    c.name;
