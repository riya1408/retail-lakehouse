# Retail Clickstream Lakehouse

Medallion pipeline (bronze -> silver -> gold) on 42M e-commerce events with an SCD2 product price history.
Question: did price changes change what customers bought?

Stack: PySpark, Databricks (Delta, Unity Catalog), DuckDB. Work in progress.

Data: "eCommerce behavior data from multi category store" (Kaggle), collected by the Open CDP project;
(c) original authors, used for non-commercial learning. Raw data is not included in this repository.
