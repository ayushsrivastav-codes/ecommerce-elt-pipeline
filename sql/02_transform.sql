USE ecommerce_dw;

DROP TABLE IF EXISTS orders_transformed;

CREATE TABLE orders_transformed (
    order_id STRING,
    customer_id STRING,
    amount DOUBLE,
    order_date DATE
)
STORED AS PARQUET;

INSERT OVERWRITE TABLE orders_transformed
SELECT
    order_id,
    customer_id,
    CAST(amount AS DOUBLE),
    CAST(order_date AS DATE)
FROM orders_raw
WHERE order_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND CAST(amount AS DOUBLE) IS NOT NULL
  AND CAST(order_date AS DATE) IS NOT NULL;
