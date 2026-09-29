from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


default_args = {
    "owner": "ubuntu",
    "depends_on_past": False,
}


with DAG(
    dag_id="ecommerce_elt",
    default_args=default_args,
    start_date=datetime(2026, 9, 28),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "elt", "hadoop", "hive"],
) as dag:

    # =========================================================
    # EXTRACT
    # =========================================================

    with TaskGroup("extract", tooltip="Extract source data") as extract:

        extract_orders = BashOperator(
            task_id="extract_orders",
            bash_command="""
            set -e

            echo "===== EXTRACT ORDERS ====="

            test -f ~/ecommerce-elt/input/orders.csv

            echo "Orders source file found:"
            ls -lh ~/ecommerce-elt/input/orders.csv
            """,
        )

        extract_customers = BashOperator(
            task_id="extract_customers",
            bash_command="""
            set -e

            echo "===== EXTRACT CUSTOMERS ====="

            sudo mysql -e "
            USE ecommerce;
            SELECT COUNT(*) AS customer_count
            FROM customers;
            "
            """,
        )

    # =========================================================
    # LOAD
    # =========================================================

    with TaskGroup("load", tooltip="Load data into HDFS") as load:

        load_orders_hdfs = BashOperator(
            task_id="load_orders_hdfs",
            bash_command="""
            set -e

            echo "===== LOAD ORDERS INTO HDFS ====="

            hdfs dfs -mkdir -p /data/raw/orders

            hdfs dfs -put -f \
            ~/ecommerce-elt/input/orders.csv \
            /data/raw/orders/

            echo "Orders in HDFS:"
            hdfs dfs -ls /data/raw/orders
            """,
        )

        load_customers_hdfs = BashOperator(
            task_id="load_customers_hdfs",
            bash_command="""
            set -e
            source ~/.ecommerce_elt_env

            echo "===== LOAD CUSTOMERS INTO HDFS ====="

            hdfs dfs -rm -r -f /data/raw/customers

            sqoop import \
            --connect jdbc:mysql://localhost:3306/ecommerce \
            --username sqoop_user \
            --password "$SQOOP_PASSWORD" \
            --table customers \
            --target-dir /data/raw/customers \
            --fields-terminated-by ',' \
            --lines-terminated-by '\\n' \
            --m 1

            echo "Customers in HDFS:"
            hdfs dfs -ls /data/raw/customers
            """,
        )

    # =========================================================
    # TRANSFORM
    # =========================================================

    with TaskGroup("transform", tooltip="Create raw Hive tables and transform data") as transform:

        create_hive_raw = BashOperator(
            task_id="create_hive_raw",
            bash_command=r"""
            set -e

            beeline -u jdbc:hive2://localhost:10000 -e "

            CREATE DATABASE IF NOT EXISTS ecommerce_dw;

            USE ecommerce_dw;

            DROP TABLE IF EXISTS orders_raw;

            CREATE EXTERNAL TABLE orders_raw (
                order_id STRING,
                customer_id STRING,
                amount STRING,
                order_date STRING
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
            LOCATION '/data/raw/orders'
            TBLPROPERTIES ('skip.header.line.count'='1');

            DROP TABLE IF EXISTS customers_raw;

            CREATE EXTERNAL TABLE customers_raw (
                customer_id STRING,
                name STRING,
                email STRING
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
            LOCATION '/data/raw/customers';

            "
            """,
        )

        transform_orders = BashOperator(
            task_id="transform_orders",
            bash_command=r"""
            set -e

            beeline -u jdbc:hive2://localhost:10000 -e "

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

            "
            """,
        )

        create_hive_raw >> transform_orders

    # =========================================================
    # ANALYTICS
    # =========================================================

    with TaskGroup("analytics", tooltip="Business analytics") as analytics:

        customer_summary = BashOperator(
            task_id="customer_summary",
            bash_command=r"""
            set -e

            beeline -u jdbc:hive2://localhost:10000 -e "

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

            "
            """,
        )

        daily_sales = BashOperator(
            task_id="daily_sales",
            bash_command=r"""
            set -e

            beeline -u jdbc:hive2://localhost:10000 -e "

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

            "
            """,
        )

        top_customers = BashOperator(
            task_id="top_customers",
            bash_command=r"""
            set -e

            beeline -u jdbc:hive2://localhost:10000 -e "

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

            "
            """,
        )

        customer_summary >> top_customers

    # =========================================================
    # DATA QUALITY
    # =========================================================

    quality_check = BashOperator(
        task_id="quality_check",
        bash_command=r"""
        set -e

        beeline -u jdbc:hive2://localhost:10000 -e "

        USE ecommerce_dw;

        SELECT
            'orders_transformed' AS table_name,
            COUNT(*) AS row_count
        FROM orders_transformed

        UNION ALL

        SELECT
            'customer_order_summary',
            COUNT(*)
        FROM customer_order_summary

        UNION ALL

        SELECT
            'daily_sales',
            COUNT(*)
        FROM daily_sales

        UNION ALL

        SELECT
            'top_customers',
            COUNT(*)
        FROM top_customers;

        SELECT
            COUNT(*) AS null_count
        FROM orders_transformed
        WHERE order_id IS NULL
           OR customer_id IS NULL
           OR amount IS NULL
           OR order_date IS NULL;

        "
        """,
    )

    # =========================================================
    # PIPELINE DEPENDENCIES
    # =========================================================

    # EXTRACT → LOAD
    extract >> load

    # LOAD → TRANSFORM
    load >> transform

    # TRANSFORM → ANALYTICS
    transform >> analytics

    # ANALYTICS → QUALITY CHECK
    analytics >> quality_check
