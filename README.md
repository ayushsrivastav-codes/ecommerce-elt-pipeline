# 🛒 E-Commerce ELT Data Engineering Pipeline

An end-to-end batch ELT data engineering project built on AWS EC2 Ubuntu Linux using Hadoop, HDFS, YARN, Hive, Sqoop, MySQL, Parquet and Apache Airflow.

## 🏗️ Architecture

The pipeline is orchestrated by **Apache Airflow**. The four processing stages are explicitly separated so it is easy to see **where extraction, loading, transformation, analytics, and data quality happen**.

```mermaid
flowchart LR

    subgraph AIRFLOW["☁️ APACHE AIRFLOW — ORCHESTRATION LAYER"]
        direction LR

        subgraph EXTRACT["1️⃣ EXTRACT"]
            direction TB
            E1["extract_orders<br/>orders.csv"]
            E2["extract_customers<br/>MySQL: ecommerce.customers"]
        end

        subgraph LOAD["2️⃣ LOAD"]
            direction TB
            L1["load_orders_hdfs<br/>HDFS RAW"]
            L2["load_customers_hdfs<br/>Sqoop → HDFS RAW"]
        end

        subgraph TRANSFORM["3️⃣ TRANSFORM"]
            direction TB
            T1["create_hive_raw<br/>orders_raw + customers_raw"]
            T2["transform_orders<br/>Hive SQL → Parquet"]
        end

        subgraph ANALYTICS["4️⃣ ANALYTICS"]
            direction TB
            A1["customer_summary"]
            A2["daily_sales"]
            A3["top_customers"]
        end

        subgraph QUALITY["5️⃣ DATA QUALITY"]
            direction TB
            Q1["quality_check<br/>row counts + NULL validation"]
        end

        EXTRACT --> LOAD --> TRANSFORM --> ANALYTICS --> QUALITY
        T1 --> T2
    end

    E1 --> L1
    E2 --> L2
    L1 --> T1
    L2 --> T1
    T2 --> A1
    T2 --> A2
    A1 --> A3
    A1 --> Q1
    A2 --> Q1
    A3 --> Q1
```

### 🔎 Stage-by-stage view

| Stage | What happens | Main technology | Output |
|---|---|---|---|
| **1. Extract** | Read orders from CSV and customers from MySQL | Python / MySQL | Source data |
| **2. Load** | Move source data into the raw HDFS layer | HDFS / Sqoop / Shell | HDFS raw files |
| **3. Transform** | Create Hive raw tables, clean/type-cast orders, store as Parquet | Hive SQL | `orders_transformed` |
| **4. Analytics** | Build customer, daily-sales and top-customer datasets | Hive SQL | Analytics Parquet tables |
| **5. Data Quality** | Validate row counts and NULL values | Hive + Airflow | Validated pipeline |
| **Orchestration** | Control dependencies, execution and monitoring | Apache Airflow | End-to-end workflow |

**Pipeline flow:** `CSV + MySQL → HDFS RAW → Hive RAW → Parquet → Analytics → Data Quality`

**Airflow task groups:** `extract → load → transform → analytics → quality_check`