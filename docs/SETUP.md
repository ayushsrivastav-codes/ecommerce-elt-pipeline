# Setup Guide

## Environment

This project runs on an AWS EC2 Ubuntu environment with Hadoop, Hive, Sqoop, MySQL, Python, and Apache Airflow.

## 1. Credentials

Create a private credentials file outside the repository:

```bash
nano ~/.ecommerce_elt_env
export SQOOP_PASSWORD="YOUR_PASSWORD_HERE"
chmod 600 ~/.ecommerce_elt_env
source ~/.ecommerce_elt_env
```

Never commit real credentials, passwords, AWS keys, or PEM files.

## 2. Load Source Data

Load orders into HDFS:

```bash
bash scripts/load_orders.sh
```

Load customers from MySQL using Sqoop:

```bash
bash scripts/sqoop_customers.sh
```

## 3. Hive Processing

Create raw Hive tables:

```bash
beeline -u jdbc:hive2://localhost:10000 -f sql/01_create_raw_tables.sql
```

Run the transformation:

```bash
beeline -u jdbc:hive2://localhost:10000 -f sql/02_transform.sql
```

Run the analytical SQL scripts:

```bash
beeline -u jdbc:hive2://localhost:10000 -f sql/03_customer_summary.sql
beeline -u jdbc:hive2://localhost:10000 -f sql/04_daily_sales.sql
beeline -u jdbc:hive2://localhost:10000 -f sql/05_top_customers.sql
```

## 4. Airflow

The pipeline is orchestrated by the `ecommerce_elt` DAG.

Check the DAG:

```bash
airflow dags list | grep ecommerce_elt
```

Trigger the pipeline:

```bash
airflow dags trigger ecommerce_elt
```

## 5. Final Validation

Expected analytical dataset counts:

| Dataset | Rows |
|---|---:|
| orders_transformed | 20 |
| customer_order_summary | 5 |
| daily_sales | 10 |
| top_customers | 5 |

Expected NULL count in `orders_transformed`: **0**.

## 6. Security

The real Sqoop password is stored outside the repository in `~/.ecommerce_elt_env`.
Only the example configuration file is committed to GitHub.
