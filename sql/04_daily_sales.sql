USE ecommerce_dw;

DROP TABLE IF EXISTS daily_sales;

CREATE TABLE daily_sales (
    order_date DATE,
    daily_revenue DOUBLE,
    order_count INT
)
STORED AS PARQUET;

INSERT OVERWRITE TABLE daily_sales
SELECT
    order_date,
    SUM(amount),
    COUNT(order_id)
FROM orders_transformed
GROUP BY order_date;
