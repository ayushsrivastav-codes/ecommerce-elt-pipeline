USE ecommerce_dw;

DROP TABLE IF EXISTS top_customers;

CREATE TABLE top_customers (
    customer_id STRING,
    name STRING,
    total_spent DOUBLE,
    total_orders INT
)
STORED AS PARQUET;

INSERT OVERWRITE TABLE top_customers
SELECT
    customer_id,
    name,
    total_spent,
    total_orders
FROM customer_order_summary
ORDER BY total_spent DESC
LIMIT 10;
