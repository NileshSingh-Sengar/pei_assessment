## Overview

A medallion-architecture (Bronze → Silver → Gold) data pipeline built on Databricks using PySpark, Delta Lake, and Unity Catalog, processing e-commerce sales data to produce profitability insights.

## Architecture

https://dbc-2cb095ca-e76e.cloud.databricks.com/editor/files/1849497942989324?o=7474660439525765$0


## Data Model

https://dbc-2cb095ca-e76e.cloud.databricks.com/editor/files/1849497942989325?o=7474660439525765$0

## Setup

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Source files: Orders.json, Products.csv, Customer.xlsx (included in data/)

### Step 1 — Import the project

In Databricks: Workspace → Import → upload the zip file.

### Step 2 — Update config.py

Open config.py and set WORKSPACE_ROOT to the exact path where you imported the project:

python
WORKSPACE_ROOT = "/Workspace/Shared/pei_assessment"  # change to match your path


### Step 3 — Run setup.py

Creates the Unity Catalog objects (catalog, schemas, volume) and installs dependencies.

### Step 4 — Upload source data files

Upload Orders.json, Products.csv, Customer.xlsx from the data/ folder to the volume:

Catalog → pei_assessment → bronze → landing → Upload to this volume

### Step 5 — Create the Databricks job

The job definition is at workflow/pei_assessment_etl.yml. Create it using Databricks UI :
Go to Workflows → Create Job and copy paste the yaml contents.

### Step 6 — Run the pipeline

Go to Workflows → pei_assessment_etl → Run now

Each task runs quality checks on completion and halts downstream tasks on failure.

## Assumptions

### Data
- Each row in Orders.json is an order line item — one order ID spans multiple rows (one per product)
- Profit is pre-calculated in the source; not derived from Price × Quantity × Discount
- All customers referenced in orders have a matching customer record
- Customers with null/empty names cannot be used meaningfully
- Phone values #ERROR! and negative numbers are corrupt source data — set to NULL
- Customer names with embedded digits or special characters (e.g. Gary567 Hansen) are dirty, which are cleaned with regex
- Duplicate product_id rows represent the same product across different states — deduplicated by keeping the lowest price row


## Design Decisions

The pipeline does a full overwrite on every run, which is correct for this bounded dataset. In production I'd use MERGE INTO with order_id as the key to handle incremental loads without full rescans.

Dimensions use SCD Type 1 since tracking historical attribute changes wasn't in scope. fact_order is partitioned by year because every insight query groups or filters on year, so partition pruning cuts scan cost meaningfully as data grows.

Profit is summed from the raw column and rounded once per aggregate rather than summing pre-rounded values — summing rounded intermediates accumulates error across large row counts.

The product deduplication keeps the lowest-price row per product_id and writes all duplicate rows to a quarantine table so they're auditable rather than silently dropped. Orders with no matching customer or product go to fact_order_quarantine for the same reason.
