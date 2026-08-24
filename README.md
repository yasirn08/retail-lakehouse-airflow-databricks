# RetailLakehouse

Production-style incremental ETL pipeline built with Apache Airflow and Databricks.

## Overview

RetailLakehouse is an end-to-end data engineering portfolio project demonstrating orchestration, incremental data ingestion, lakehouse architecture, data transformation, data quality, and dimensional modelling.

## Tech Stack

- Apache Airflow
- Databricks
- PySpark
- Spark SQL
- Delta Lake
- Python
- PostgreSQL
- Docker
- GitHub Actions

## Architecture

The pipeline will follow a medallion architecture:

Source → Airflow → Bronze → Silver → Gold → Analytics

## Project Structure

- `airflow/` — orchestration and DAGs
- `databricks/` — Bronze, Silver, and Gold transformations
- `src/` — reusable Python modules
- `sql/` — SQL scripts
- `tests/` — automated tests
- `docs/` — architecture and documentation
- `data/` — local development data

## Status

🚧 Currently under development.