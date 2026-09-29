# E-Commerce ELT Pipeline

An end-to-end e-commerce data engineering project that extracts data from MySQL and CSV sources, loads it into HDFS, transforms and analyzes it using Apache Hive, stores analytical datasets in Parquet, and orchestrates the complete workflow with Apache Airflow.

## Project Overview

This project demonstrates a complete batch ELT pipeline:

MySQL / CSV → Sqoop / Shell → HDFS → Hive → Parquet → Airflow Data Quality Checks

The pipeline processes customer and order data and produces analytical datasets for customer spending, daily sales, and top customers.

## Architecture

```text
                 ┌──────────────────┐
                 │   Data Sources   │
                 │                  │
                 │ MySQL Customers  │
                 │ CSV Orders       │
                 └────────┬─────────┘
                          │
                 ┌────────▼─────────┐
                 │ Extraction/Load  │
                 │                  │
                 │ Sqoop + Bash     │
                 └────────┬─────────┘
                          │
                 ┌────────▼─────────┐
                 │      HDFS        │
                 │    RAW Layer     │
                 └────────┬─────────┘
                          │
                 ┌────────▼─────────┐
                 │      Hive        │
                 │   RAW Tables     │
                 └────────┬─────────┘
                          │
                 ┌────────▼─────────┐
                 │ Transformations  │
                 │                  │
                 │ SQL + Parquet    │
                 └────────┬─────────┘
                          │
             ┌────────────┼────────────┐
             │            │            │
             ▼            ▼            ▼
        Customer      Daily Sales   Top Customers
         Summary
             │            │            │
             └────────────┼────────────┘
                          │
                 ┌────────▼─────────┐
                 │  Data Quality    │
                 │   Checks         │
                 │                  │
                 │ 20 orders        │
                 │ 0 NULL records   │
                 └────────┬─────────┘
                          │
                 ┌────────▼─────────┐
                 │ Apache Airflow   │
                 │   Orchestration  │
                 └──────────────────┘
