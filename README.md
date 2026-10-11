# Procurement Lakehouse on Databricks

A medallion lakehouse (bronze, silver, gold) for synthetic procure-to-pay data:
suppliers, purchase orders, goods receipts and invoices. Built on Databricks Free Edition
with Unity Catalog, Auto Loader, Delta Lake, MERGE, SCD Type 2, Lakeflow Declarative
Pipelines and Lakeflow Jobs.

Work in progress. The full README (architecture diagram, how to run, design decisions,
what I'd add in production) is written in phase 9.

## Quick start

1. Clone this repo into a Databricks Git folder.
2. Run `setup/00_setup.sql` on serverless compute.
