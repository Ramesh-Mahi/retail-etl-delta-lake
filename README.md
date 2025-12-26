# retail-etl-delta-lake
“I have a complete PySpark Delta Lake ETL project on GitHub where I implemented Bronze, Silver, and Gold layers, SCD Type-2 dimensions, and incremental fact loading
# Retail ETL Pipeline using PySpark & Delta Lake

## Overview
This project implements an end-to-end batch ETL pipeline using PySpark and Delta Lake
following the Medallion Architecture (Bronze, Silver, Gold).

## Architecture
Bronze → Silver → Gold

## Bronze Layer
- Raw data ingestion from CSV files
- Append-only Delta tables
- Ingestion timestamp added for traceability

## Silver Layer
- Data cleansing and deduplication
- Latest record selection using window functions
- Ensures trusted datasets

## Gold Layer
- Customer dimension implemented using SCD Type-2
- Fact orders table created for analytics

## Incremental Load Logic
- Fact table is loaded incrementally
- Maximum processed order_date is read from existing Delta table
- Only new records are processed in subsequent runs

## Technologies Used
- PySpark
- Delta Lake
- SQL
- Window Functions

## How to Run
1. Place input CSV files under data/sample_inputs
2. Run Bronze layer scripts
3. Run Silver layer scripts
4. Run Gold layer scripts
