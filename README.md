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

### End-to-End Pipeline Flow

```text
Cloud Storage (raw/ trigger)
        ↓
Pub/Sub Topic
        ↓
Cloud Function (Gen2)
        ↓
Cloud Composer / Airflow
        ↓
PySpark Jobs on Dataproc
        ↓
BigQuery (Staging → Dimensions → Facts)
```

---

## Key Features

### Event-Driven Processing

* Automatic pipeline triggering on file arrival in **GCS**
* No manual DAG execution required

### Orchestrated ETL using Airflow

* Multi-stage DAG orchestration using **Cloud Composer**
* Sequential task dependencies

### Dynamic Dataproc Clusters

* Ephemeral **Dataproc clusters**
* Cluster created for each processing stage
* Cluster automatically deleted after job completion
* Cost-efficient processing approach

### PySpark-Based Data Processing

* Distributed transformation logic using **PySpark**
* Large-scale healthcare claims data processing

### Slowly Changing Dimension (SCD Type 2)

* Implemented **SCD Type 2** on the **Provider Dimension**
* Historical provider changes tracked

### Data Quality Validation

* Null checks
* Duplicate detection
* Record count validations
* Pipeline quality gates

---

## Dataset

This project uses the **CMS Medicare Part D Prescriber Public Use File (PUF)** dataset.

The data contains provider-level prescription claim information including:

* Provider identifiers
* Drug claims
* Specialty information
* Prescription counts
* Geographic metadata

---

## Tech Stack

| Category               | Technologies                    |
| ---------------------- | ------------------------------- |
| Cloud Platform         | Google Cloud Platform (GCP)     |
| Storage                | Google Cloud Storage            |
| Event Streaming        | Pub/Sub                         |
| Serverless Trigger     | Cloud Functions Gen2            |
| Workflow Orchestration | Cloud Composer / Apache Airflow |
| Processing Engine      | Dataproc                        |
| Data Processing        | PySpark                         |
| Data Warehouse         | BigQuery                        |
| Programming Language   | Python                          |
| Query Language         | SQL                             |

---

## Pipeline Architecture

### Stage 1 — Raw Ingestion

**Purpose:** Load raw Medicare Part D files into the staging layer.

**Actions:**

* Read raw source files from GCS
* Standardize schema
* Initial ingestion into BigQuery staging

**Output Table:**

```sql
staging_layer.stg_prescribers
```

---

### Stage 2 — Data Cleansing

**Purpose:** Clean and standardize incoming healthcare data.

**Transformations:**

* Null handling
* Data type standardization
* Duplicate removal
* Column normalization
* Schema enforcement

---

### Stage 3 — SCD Type 2 Merge

**Purpose:** Build historical provider dimension.

**Implemented:**

**Slowly Changing Dimension Type 2**

Tracks:

* Historical provider information
* Change timestamps
* Active/inactive records

**Output Table:**

```sql
dim_tables.dim_provider
```

---

### Stage 4 — Data Quality Checks

**Purpose:** Validate pipeline output.

Checks include:

* Row count validation
* Duplicate checks
* Missing value checks
* Schema validation

---

## BigQuery Data Model

### Staging Layer

```text
staging_layer
└── stg_prescribers
```

### Dimension Layer

```text
dim_tables
└── dim_provider
```

### Fact Layer

```text
fact_tables
└── fct_claims
```

---

## Event-Driven Trigger Workflow

The pipeline is fully automated.

### Trigger Flow

1. File uploaded to:

```text
gs://cms-partd-data-demo/raw/
```

2. **Cloud Storage Notification** fires

3. Event published to:

```text
Pub/Sub Topic:
medicare-file-arrival
```

4. **Cloud Function (Gen2)** receives event

5. Function triggers Airflow DAG using Composer REST API

6. Airflow orchestrates Dataproc jobs

7. Results written into BigQuery

---

## Project Structure

```text
project-root/
│
├── composer_dags/
│   ├── main_pipeline_orchestrator.py
│   ├── stage_1_raw_ingestion.py
│   ├── stage_2_data_cleansing.py
│   ├── stage_3_scd_type2_merge.py
│   └── stage_4_dq_checks.py
│
├── airflow-trigger/
│   ├── main.py
│   ├── requirements.txt
│
├── images/
│   ├── architecture-diagram.png
│   ├── airflow-dag.png
│   ├── dataproc-job.png
│   └── bigquery-results.png
│
└── README.md
```

---

## Sample Validation Queries

### Row Count Validation

```sql
SELECT COUNT(*) 
FROM staging_layer.stg_prescribers;
```

```sql
SELECT COUNT(*) 
FROM dim_tables.dim_provider;
```

```sql
SELECT COUNT(*) 
FROM fact_tables.fct_claims;
```

### BigQuery CLI Commands

```bash
bq query --use_legacy_sql=false \
'SELECT COUNT(*) 
FROM staging_layer.stg_prescribers;'
```

```bash
bq query --use_legacy_sql=false \
'SELECT COUNT(*) 
FROM dim_tables.dim_provider;'
```

```bash
bq query --use_legacy_sql=false \
'SELECT COUNT(*) 
FROM fact_tables.fct_claims;'
```

---



### BigQuery Output Tables

<img width="926" height="422" alt="Screenshot 2026-06-05 021607" src="https://github.com/user-attachments/assets/3291fd6a-4746-4345-b213-f7ed7adaf188" />
<img width="972" height="483" alt="Screenshot 2026-06-05 021556" src="https://github.com/user-attachments/assets/f1a494c4-3d58-445d-8a76-d585583a7b12" />

---

## Key Engineering Decisions

### Why Pub/Sub?

Used to decouple storage events from orchestration logic and enable event-driven processing.

### Why Cloud Functions?

Provides lightweight serverless orchestration for triggering Airflow DAGs without infrastructure management.

### Why Ephemeral Dataproc Clusters?

To reduce infrastructure costs by creating clusters only during execution and deleting them afterward.

### Why SCD Type 2?

Healthcare provider information may evolve over time, making historical tracking necessary.

---

## Resume Highlights

* Built an **event-driven healthcare data pipeline on GCP** using **Pub/Sub, Cloud Functions, Airflow, Dataproc, PySpark, and BigQuery**
* Automated orchestration using **Cloud Composer (Apache Airflow)** with multi-stage ETL DAGs
* Implemented **SCD Type 2 dimensional modeling** for historical provider tracking
* Designed **ephemeral Dataproc cluster execution** for cost-efficient distributed processing
* Processed **Medicare Part D prescriber claims data** through staging, cleansing, transformation, and validation layers
* Developed automated **data quality validation checks** to improve pipeline reliability

---

## Future Improvements

* Add CI/CD deployment pipeline
* Integrate monitoring with Cloud Monitoring
* Add data lineage tracking
* Add Terraform infrastructure provisioning
* Add dbt transformation layer


---

