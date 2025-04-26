# azure-medallion-architecture-pipeline
✅ End-to-End Azure Data Pipeline with ADF, Databricks, Synapse, and Power BI


# Azure End-to-End Data Pipeline: Bronze → Silver → Gold

This project demonstrates a real-world Azure data pipeline, using the Bronze-Silver-Gold architecture pattern, covering ingestion, transformation, storage, and reporting stages.

## 🛠️ Tech Stack
- **Azure Data Factory (ADF)**: Data ingestion
- **Azure Data Lake Storage Gen2 (ADLS)**: Storage
- **Azure Databricks**: Data transformation
- **Azure Synapse Analytics**: Data serving
- **Power BI Desktop**: Data visualization
- **Access Control (IAM)**: Secrets & Role Management

---

## 📈 Architecture Overview

1. **Bronze Layer**:
   - Dynamic ingestion using ADF.
   - `Lookup` activity to read a control JSON file containing:
     - URL
     - Sink folder path
     - Sink file path
   - `ForEach` activity loops through URLs and triggers `CopyActivity` to download files to **ADLS Bronze container**.

2. **Silver Layer**:
   - Databricks notebook reads raw files from Bronze container.
   - Applies necessary transformations (e.g., cleaning, filtering, renaming).
   - Writes cleaned data as **Parquet files** into Silver container in ADLS.

3. **Gold Layer**:
   - Synapse reads Silver parquet files using `OPENROWSET` for ad-hoc querying.
   - Creates **views** and **external tables** over Silver data.
   - Writes curated gold-standard data into Gold container.

4. **Reporting**:
   - Power BI connects to Synapse external tables and/or Gold container.
   - Interactive dashboards and reports are created.

---

## 📂 Project Folder Structure
data-factory/ pipeline.json linked-services.json
databricks/ bronze_to_silver_notebook.py
synapse/ gold_external_tables.sql views_creation.sql
resources/ lookup_file.json


## ⚙️ Setup Instructions

1. Deploy Azure resources (Data Factory, Databricks Workspace, ADLS Gen2, Synapse Workspace).
2. Upload sample `lookup_file.json` to ADF dataset.
3. Import the ADF pipeline and linked services.
4. Run the ADF pipeline to download data into the Bronze container.
5. Execute the Databricks notebook to create the Silver layer.
6. Run the Synapse SQL scripts to create external tables over Silver data into Gold layer.
7. Connect Power BI to Synapse external tables for reporting.

---
