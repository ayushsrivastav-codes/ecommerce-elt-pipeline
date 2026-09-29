# System Architecture

## 1. High-Level Architecture

```text
                    E-COMMERCE DATA PIPELINE

 ┌─────────────────────┐       ┌─────────────────────┐
 │      MySQL          │       │     orders.csv      │
 │                     │       │                     │
 │ ecommerce.customers │       │   Order Data        │
 └──────────┬──────────┘       └──────────┬──────────┘
            │                             │
            │ Sqoop                       │ Bash
            │                             │
            └──────────────┬──────────────┘
                           │
                           ▼
                 ┌───────────────────┐
                 │       HDFS        │
                 │                   │
                 │    RAW DATA       │
                 │                   │
                 │ /data/raw/orders  │
                 │ /data/raw/customers│
                 └─────────┬─────────┘
                           │
                           ▼
                 ┌───────────────────┐
                 │       Hive        │
                 │                   │
                 │   RAW TABLES      │
                 │                   │
                 │ orders_raw        │
                 │ customers_raw     │
                 └─────────┬─────────┘
                           │
                           ▼
                 ┌───────────────────┐
                 │   Transformation  │
                 │      Layer        │
                 │                   │
                 │ SQL + Hive         │
                 │ Type Conversion    │
                 │ Data Validation    │
                 └─────────┬─────────┘
                           │
                           ▼
                 ┌───────────────────┐
                 │   Parquet Layer   │
                 │                   │
                 │ orders_transformed│
                 └─────────┬─────────┘
                           │
             ┌─────────────┼─────────────┐
             │             │             │
             ▼             ▼             ▼
      ┌─────────────┐ ┌────────────┐ ┌──────────────┐
      │  Customer   │ │   Daily    │ │     Top      │
      │   Summary   │ │   Sales    │ │  Customers   │
      └─────────────┘ └────────────┘ └──────────────┘
             │             │             │
             └─────────────┼─────────────┘
                           ▼
                 ┌───────────────────┐
                 │   Data Quality     │
                 │      Checks        │
                 │                   │
                 │ Row Counts        │
                 │ NULL Validation   │
                 └─────────┬─────────┘
                           │
                           ▼
                 ┌───────────────────┐
                 │     Airflow       │
                 │   Orchestration   │
                 └───────────────────┘
