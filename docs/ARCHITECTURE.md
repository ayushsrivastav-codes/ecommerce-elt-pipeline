# 🛒 E-Commerce ELT Pipeline Architecture

## High-Level Architecture

Apache Airflow is the **outer orchestration layer**.  
Inside the Airflow workflow, the pipeline is divided into five clearly defined stages.

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│                         APACHE AIRFLOW                                       │
│                    ORCHESTRATION LAYER                                       │
│                                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌────────────────┐                │
│  │  1. EXTRACT  │───▶│   2. LOAD    │───▶│  3. TRANSFORM  │                │
│  │              │    │              │    │                │                │
│  │ orders.csv   │    │ HDFS RAW     │    │ Hive SQL       │                │
│  │ MySQL        │    │              │    │ Type casting   │                │
│  │              │    │ Sqoop        │    │ Data cleaning  │                │
│  │ Bash / Sqoop │    │ Bash / HDFS  │    │ Parquet        │                │
│  └──────────────┘    └──────────────┘    └───────┬────────┘                │
│                                                  │                          │
│                                                  ▼                          │
│                                         ┌────────────────┐                  │
│                                         │ 4. ANALYTICS   │                  │
│                                         │                │                  │
│                                         │ Customer       │                  │
│                                         │ Summary        │                  │
│                                         │                │                  │
│                                         │ Daily Sales    │                  │
│                                         │                │                  │
│                                         │ Top Customers  │                  │
│                                         └───────┬────────┘                  │
│                                                 │                           │
│                                                 ▼                           │
│                                         ┌────────────────┐                  │
│                                         │ 5. DATA        │                  │
│                                         │    QUALITY     │                  │
│                                         │                │                  │
│                                         │ Row Counts     │                  │
│                                         │ NULL Checks    │                  │
│                                         │ Pipeline       │                  │
│                                         │ Validation     │                  │
│                                         └────────────────┘                  │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘

