# banking-data-engineering
End-to-End Banking Data Engineering Project using Apache Beam, GCP, BigQuery and Airflow
# Banking Data Engineering Project

## Overview
This project demonstrates an end-to-end Data Engineering pipeline using Apache Beam, BigQuery, and Apache Airflow.

## Tech Stack
- Python
- SQL
- Apache Beam
- Google Cloud Storage
- BigQuery
- Apache Airflow
- GitHub

## Architecture

CSV Files
(Customers, Accounts, Transactions)
        ↓
Apache Beam Pipelines
        ↓
BigQuery Bronze Layer
        ↓
BigQuery Silver Layer
        ↓
BigQuery Gold Layer
        ↓
Apache Airflow Orchestration

## Bronze Layer
- customers
- accounts
- transactions

## Silver Layer
- customer_accounts
- customer_transactions
- transaction_summary

## Gold Layer
- customer_dashboard

## Airflow
Used Apache Airflow to orchestrate and schedule pipeline execution.

## Key Features
- Data Validation
- Data Transformation
- Medallion Architecture
- Automated Workflow Management
- BigQuery Analytics

## Author
Akshay Reddy
