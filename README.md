# Azure Data Engineering Stock Pipeline

This project demonstrates an end-to-end Azure Data Engineering pipeline that ingests stock market data, stores it in a partitioned data lake, and loads it into a dimensional data warehouse for analytics.
The architecture follows a modern medallion-style data pipeline:

RAW → STG → Data Lake (Silver) → Data Warehouse (Dim/Fact)

The solution is implemented using Azure Data Factory, Azure SQL Database, and Azure Data Lake Gen2.

# Project Architecture

