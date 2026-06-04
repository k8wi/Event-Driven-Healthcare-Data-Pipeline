# Event-Driven Healthcare Data Pipeline (GCP + Airflow + PySpark)

> A production-style, event-driven healthcare data engineering pipeline built on Google Cloud Platform using Airflow, PySpark, Dataproc, Pub/Sub, Cloud Functions, and BigQuery to process **Medicare Part D prescriber claims data**.

---

## Project Overview

This project demonstrates an **end-to-end event-driven healthcare data pipeline** built using **Google Cloud Platform (GCP)** to process and transform **Medicare Part D Prescriber Data**.

The architecture simulates a real-world cloud data engineering workflow where:

* A new file landing in **Google Cloud Storage (GCS)** automatically triggers the pipeline
* **Pub/Sub** captures file arrival events
* A **Cloud Function (Gen2)** triggers an **Apache Airflow DAG** in **Cloud Composer**
* **Dataproc clusters** are created dynamically for PySpark processing and deleted after execution
* Cleaned and modeled datasets are stored in **BigQuery** for analytics

The pipeline follows a **multi-stage ETL architecture** with raw ingestion, cleansing, dimensional modeling, and data quality validation.

---

## Architecture

<img width="1316" height="2079" alt="architecture" src="https://github.com/user-attachments/assets/7293443b-a302-4df1-ac09-4a106bb49c92" />

### Airflow DAG Execution

<img width="1879" height="796" alt="Screenshot 2026-06-05 021155" src="https://github.com/user-attachments/assets/e34b22ce-dbd2-460c-88c6-b1b583ac9526" />

````md
## Pipeline Overview

An **event-driven healthcare data pipeline** built on **Google Cloud Platform (GCP)** to process **CMS Medicare Part D prescriber claims data** using **Airflow, PySpark, Dataproc, Pub/Sub, Cloud Functions, and BigQuery**.

### End-to-End Flow

```text
Cloud Storage (raw/)
      ↓
Pub/Sub Trigger
      ↓
Cloud Function (Gen2)
      ↓
Cloud Composer / Airflow
      ↓
PySpark Jobs on Dataproc
      ↓
BigQuery (Staging → Dimensions → Facts)
````

### Key Features

✅ **Event-driven ingestion** — pipeline auto-triggers when files land in GCS

✅ **Airflow orchestration** — multi-stage ETL with Cloud Composer

✅ **Ephemeral Dataproc clusters** — created & deleted per stage for cost optimization

✅ **PySpark transformations** — scalable healthcare claims processing

✅ **SCD Type 2 implementation** — historical tracking in `dim_provider`

✅ **Data quality checks** — row counts, duplicates, schema & null validation

---

## Tech Stack

| Layer         | Technology               |
| ------------- | ------------------------ |
| Cloud         | GCP                      |
| Storage       | Cloud Storage            |
| Triggering    | Pub/Sub                  |
| Serverless    | Cloud Functions Gen2     |
| Orchestration | Cloud Composer (Airflow) |
| Processing    | Dataproc + PySpark       |
| Warehouse     | BigQuery                 |
| Language      | Python + SQL             |

---

## Data Model

```text
staging_layer
└── stg_prescribers

dim_tables
└── dim_provider (SCD Type 2)

fact_tables
└── fct_claims
```

---

## Pipeline Stages

| Stage   | Purpose                                  |
| ------- | ---------------------------------------- |
| Stage 1 | Raw ingestion into staging               |
| Stage 2 | Data cleansing & standardization         |
| Stage 3 | SCD Type 2 merge into provider dimension |
| Stage 4 | Data quality validation                  |

---

## Sample Validation Queries

```sql
SELECT COUNT(*) FROM staging_layer.stg_prescribers;
SELECT COUNT(*) FROM dim_tables.dim_provider;
SELECT COUNT(*) FROM fact_tables.fct_claims;
```

```
```



---

