# 🛒 E-Commerce ELT Data Engineering Pipeline

An end-to-end batch ELT data engineering project built on AWS EC2 Ubuntu Linux using Hadoop, HDFS, YARN, Hive, Sqoop, MySQL, Parquet and Apache Airflow.

## 📌 Project Question

**How can we build a reliable automated pipeline that combines e-commerce orders from CSV with customer data from MySQL and produces validated analytical datasets for business reporting?**

## 🎯 Objectives

- Extract orders from CSV
- Extract customers from MySQL
- Load raw data into HDFS
- Use Sqoop for MySQL ingestion
- Create Hive raw tables
- Transform data using Hive SQL
- Store curated data as Parquet
- Generate customer, daily-sales and top-customer analytics
- Perform data-quality validation
- Orchestrate the workflow with Apache Airflow
- Run the complete environment on AWS EC2 Ubuntu Linux

## 🏗️ Architecture

```text
                         AWS EC2
                    Ubuntu Linux Server
                            |
                     APACHE AIRFLOW
                    ORCHESTRATION
                            |
              +-------------+-------------+
              |             |             |
           EXTRACT         LOAD        TRANSFORM
              |             |             |
              +-------------+-------------+
                            |
              +-------------+-------------+
              |                           |
              v                           v
         orders.csv                    MySQL
              |                           |
              v                           v
        HDFS RAW                      customers
              |                           |
              |                         Sqoop
              |                           |
              +-------------+-------------+
                            |
                            v
                         HDFS RAW
                            |
                +-----------+-----------+
                |                       |
                v                       v
           orders_raw             customers_raw
                |                       |
                +-----------+-----------+
                            |
                            v
                   HIVE TRANSFORMATION
                            |
                            v
                  orders_transformed
                       PARQUET
                            |
              +-------------+-------------+
              |             |             |
              v             v             v
        Customer       Daily Sales   Top Customers
         Summary
              |             |             |
              +-------------+-------------+
                            |
                            v
                      DATA QUALITY
                            |
                            v
                         VALIDATED

