# Healthcare Metrics Pipeline — Solution Design Document

**Project:** Healthcare Metrics Pipeline  
**Version:** 2.0 — Revised per SME Feedback  
**Prepared by:** Mariano Beccaria
**Date:** April 2026  
**Status:** SME Approved

---

## Revision History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | April 2026 | Initial draft — Lambda + Step Functions + Parquet |
| 2.0 | April 2026 | Revised per SME feedback — Glue Workflow + Delta Lake |

**SME Feedback (v1.0 → v2.0):**
> "There are good analysis reports on your repository but the architecture needs
> to be better. We can orchestrate the entire flow on AWS Glue and also not
> seeing a requirement of Lambda for the same. Use Glue Workflow and maybe
> promote your Spark code to implement Delta Lake in S3."

**Changes made in v2.0:**
- Removed AWS Lambda (ingestion now handled by Glue Python Shell job)
- Removed AWS Step Functions (orchestration now handled by Glue Workflow)
- Removed Amazon EventBridge (scheduling now handled by Glue Workflow trigger)
- Added Delta Lake format to Silver and Gold layers (replaces plain Parquet)
- Added Glue Workflow as the single orchestration layer

---

## 1. Executive Summary

This document describes the AWS data pipeline architecture for the Healthcare
Metrics project. The pipeline ingests CMS nursing home staffing data from Google
Drive, transforms it through a three-layer Delta Lake on S3, calculates key
staffing and quality metrics, and surfaces insights via an interactive Streamlit
dashboard.

The architecture is unified entirely within AWS Glue — a single Glue Workflow
orchestrates ingestion, transformation, and metric calculation. Delta Lake on S3
provides ACID transactions, time travel, and native incremental merge capability,
replacing the manual watermark pattern from v1.0.

---

## 2. Data Sources

| File | Description | Rows (Q2 2024) | Join Key |
|------|-------------|---------------|----------|
| `PBJ_Daily_Nurse_Staffing_Q2_2024.csv` | Daily staffing hours per facility | 1,325,324 | PROVNUM |
| `NH_ProviderInfo_*.csv` | Facility details, ratings, bed counts | 14,814 | CCN |
| `NH_QualityMsr_MDS_*.csv` | MDS quality measures per facility | ~300,000 | CCN |
| `NH_QualityMsr_Claims_*.csv` | Medicare claims quality measures | ~150,000 | CCN |
| `FY_2024_SNF_VBP_*.csv` | Value-based purchasing scores | ~15,000 | CCN |
| `NH_HealthCitations_*.csv` | Health inspection deficiencies | ~200,000 | CCN |
| `NH_Penalties_*.csv` | Fines and payment denials | ~50,000 | CCN |
| *(+ 9 additional supporting files)* | Ownership, surveys, COVID vax | varies | CCN |

**Source location:** Google Drive  
**Download link:** https://drive.google.com/drive/folders/15KqJ1MZ7JcgAkOfqcaWcALWkG0dh3jpE  
**Update frequency:** Quarterly (CMS releases new data each quarter)

**Key data quality findings from Step 2 EDA:**
- Zero null values across all 33 columns in the PBJ file
- Zero duplicate PROVNUM + WorkDate combinations
- 99.9% join match rate between PBJ and ProviderInfo (17 unmatched CCNs)
- Encoding: all CMS files require latin-1 encoding (Windows-1252 characters in facility names)
- Edge case: rows with MDScensus < 10 indicate reopening facilities and must be excluded from ratio calculations

---

## 3. Architecture Overview

The pipeline follows a Medallion Architecture (Bronze to Silver to Gold),
fully hosted on AWS, orchestrated entirely within AWS Glue.

```
Google Drive
     |
     |  (Glue Workflow — quarterly schedule)
     v
+--------------------------------------------------+
|              AWS Glue Workflow                    |
|                                                   |
|  Job 1: Glue Python Shell                         |
|  Google Drive -> S3 Bronze                        |
|          |                                        |
|          v  (Glue Trigger)                        |
|  Job 2: Glue Spark                                |
|  S3 Bronze -> S3 Silver (Delta Lake)              |
|          |                                        |
|          v  (Glue Trigger)                        |
|  Job 3: Glue Spark                                |
|  S3 Silver -> S3 Gold (Delta Lake)                |
+--------------------------------------------------+
     |
     v
Amazon Athena -- SQL queries on Gold Delta Lake
     |
     v
Streamlit Dashboard -- Hosted on EC2
```

**Orchestration:** AWS Glue Workflow with Glue Triggers chaining all three jobs  
**Monitoring:** AWS CloudWatch for Glue job logs, errors, and pipeline alerts

---

## 4. AWS Services — Selection and Rationale

| Service | Role | Why this service |
|---------|------|-----------------|
| **AWS Glue Workflow** | Full pipeline orchestration | Single service for scheduling, job chaining, and dependency management — eliminates need for Lambda and Step Functions |
| **Glue Python Shell job** | Incremental ingestion from Google Drive | Lightweight Python environment inside Glue — connects to Drive API, checks for new quarters, downloads to Bronze |
| **Glue Spark job (x2)** | Bronze to Silver and Silver to Gold transformation | Distributed PySpark handles 1.3M+ rows efficiently; native Delta Lake support via delta-core library |
| **Amazon S3** | Data lake storage (all three layers) | Cost-effective, durable, natively integrates with Glue and Athena |
| **Delta Lake on S3** | Table format for Silver and Gold | ACID transactions, time travel, incremental merge — replaces plain Parquet and manual watermark logic |
| **Amazon Athena** | SQL querying of Gold layer | Serverless, pay-per-query, supports Delta Lake via manifest files |
| **AWS CloudWatch** | Logging and alerting | Central log aggregation, Glue job failure alerts |
| **EC2** | Streamlit dashboard hosting | Simple deployment for project scope |

**Why Glue Workflow over Step Functions + Lambda:**  
AWS Glue Workflow provides native job chaining, scheduling, and dependency
management entirely within the Glue service. For a pipeline where all
processing happens in Glue jobs, adding Lambda and Step Functions introduces
unnecessary service complexity. Glue Workflow keeps the architecture unified
and reduces the number of IAM roles, services, and failure points to manage.

**Why Delta Lake over plain Parquet:**  
Delta Lake adds a transaction log (_delta_log/) on top of S3 Parquet files,
providing ACID guarantees and native MERGE operations. For quarterly
incremental ingestion, Delta's merge capability means new quarters are appended
to existing tables without rewriting historical data. Time travel also enables
querying previous quarters for trend analysis without maintaining separate table
copies.

**Why Athena over Redshift:**  
At 1.3M rows per quarter, a full Redshift cluster is overprovisioned and
costly. Athena queries S3 directly with no warehouse to manage, and at this
data volume query performance is sufficient for dashboard use.

---

## 5. Data Lake Layers

### 5.1 Bronze Layer — Raw Ingestion

- Files land in S3 exactly as downloaded from Google Drive
- Original filenames preserved
- No transformations applied — this is the permanent audit record
- Partitioned by: quarter=2024Q2/
- Encoding preserved as-is (latin-1)
- Format: CSV — raw files untouched

### 5.2 Silver Layer — Cleaned, Validated, Delta Lake

Glue Spark job applies the following transformations informed by Step 2 EDA:

| Transformation | Reason |
|----------------|--------|
| Load PROVNUM and CCN as string | Preserve leading zeros (e.g. 015009) |
| Apply latin-1 encoding | CMS files contain Windows-1252 characters |
| Filter MDScensus < 10 | Exclude reopening/edge case facilities |
| Route 17 unmatched CCNs to audit table | Preserve data, flag for investigation |
| Join PBJ to ProviderInfo (LEFT JOIN) | Retain all staffing records, add facility context |
| Parse WorkDate as date type | Enable time-series analysis |
| Convert hours columns to float | Ensure consistent numeric types |
| Write as Delta Lake table | ACID transactions, incremental merge, time travel |
| Partition by STATE | Optimise state-level dashboard queries |

**Delta Lake merge strategy (incremental):**

New quarters are merged into the existing Silver table so historical data is
never rewritten. The MERGE operation inserts only rows where PROVNUM and
WorkDate do not already exist in Silver.

### 5.3 Gold Layer — Metrics, Delta Lake

Glue Spark job calculates metrics from Silver, written as Delta Lake tables.

**Staffing ratio metrics (daily, per facility):**
- CNA_hrs_per_patient = Hrs_CNA / MDScensus
- RN_hrs_per_patient = Hrs_RN / MDScensus
- total_hrs_per_patient = (Hrs_RN + Hrs_LPN + Hrs_CNA) / MDScensus
- staffing_tier = categorised against CMS thresholds

**CMS minimum thresholds applied:**

| Staff Type | CMS Minimum | Source |
|-----------|-------------|--------|
| CNA | 2.45 hrs/patient/day | CMS 2024 rule |
| RN | 0.55 hrs/patient/day | CMS 2024 rule |
| Total nurse | 3.48 hrs/patient/day | CMS 2024 rule |

**Facility-level aggregations (per quarter):**
- Average staffing ratios by facility, state, ownership type
- Days below CMS minimum per facility
- Chronic understaffing flag (>50% of days below minimum)
- Contracted vs employed hours ratio
- Bed occupancy rate (MDScensus / certified_beds)

**Quality correlation metrics (joined with MDS file):**
- Correlation: CNA ratio vs pressure ulcer rate
- Correlation: RN ratio vs fall injury rate
- Correlation: total hours vs UTI rate

---

## 6. Incremental Ingestion Design

CMS releases new quarterly data approximately every 3 months. The Glue Python
Shell job handles incremental detection using Delta Lake transaction history
rather than a manual watermark file.

**Incremental flow:**
1. Glue Workflow triggers on schedule (quarterly)
2. Python Shell job connects to Google Drive API
3. Job reads Delta Lake history to find last ingested quarter
4. If new quarter exists on Drive: download CSVs to Bronze, trigger next job
5. If no new quarter: log and exit — Glue Workflow stops cleanly

**Delta Lake replaces the manual watermark:**  
In v1.0, a last_run.json file in S3 tracked the last ingested quarter.
In v2.0, Delta Lake's transaction log (_delta_log/) serves this purpose
natively — the job queries Delta history to find the latest quarter already
present in Silver, then compares against Drive.

---

## 7. S3 Bucket Structure

**Bucket:** mbeccaria-dea-healthcare-metrics

```
mbeccaria-dea-healthcare-metrics/
├── bronze/
│   └── quarter=2024Q2/          <- 21 CSV files (raw, latin-1)
├── silver/
│   └── staffing/                <- Delta Lake table
│       ├── part-*.parquet
│       └── _delta_log/          <- Delta transaction log
├── gold/
│   ├── staffing_metrics/        <- Delta Lake table
│   │   ├── part-*.parquet
│   │   └── _delta_log/
│   ├── facility_summary/        <- Delta Lake table
│   └── quality_correlations/    <- Delta Lake table
├── audit/
│   └── unmatched_ccn/           <- 17 unmatched facilities
└── scripts/
    ├── glue_ingestion.py        <- Python Shell job script
    ├── glue_bronze_to_silver.py <- Spark job script
    └── glue_silver_to_gold.py   <- Spark job script
```

---

## 8. Key Data Findings from Step 2 (EDA)

| Finding | Architecture Implication |
|---------|------------------------|
| 1,325,324 rows per quarter | Glue Spark required — pandas too slow at scale |
| Zero nulls, zero duplicates | No repair/deduplication layer needed in Silver |
| 99.9% join match rate | LEFT JOIN confirmed; 17 unmatched CCNs -> audit table |
| latin-1 encoding required | Must be specified in all Glue read operations |
| MDScensus < 10 edge cases | Silver layer filter rule |
| Only 24.5% of days meet CMS minimums | Primary dashboard KPI |
| Texas facilities — entire quarter understaffed | State-level partitioning enables efficient filtering |
| Length of stay not in PBJ | Medicare Claims QM file included in pipeline scope |

---

## 9. Questions Answered by This Pipeline

| Project Question | Data Required | Available |
|----------------|---------------|-----------|
| Staffing vs occupancy rates | PBJ + ProviderInfo (beds) | Yes |
| Highest overtime hours | PBJ (emp vs ctr ratio) | Yes |
| Avg staffing by state and type | PBJ + ProviderInfo (ownership) | Yes |
| Length of stay trends | Medicare Claims QM file | Yes — included in scope |

---

## 10. Risks and Mitigations

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Google Drive API rate limits | Medium | Retry logic with exponential backoff in Python Shell job |
| CMS changes column names between quarters | Low-Medium | Silver job schema validation — pipeline halts on unexpected columns |
| New quarter data quality issues | Medium | Silver validation runs before Gold — pipeline stops on failures |
| Delta Lake compatibility with Athena | Low | Use Delta manifest files or AWS Glue Data Catalog for Athena access |
| Streamlit dashboard slow on large queries | Low | Gold layer pre-aggregated; Athena on Delta Parquet partitions is fast |
| AWS costs exceed budget | Low | Glue pay-per-use; no always-on services in this architecture |

---

## 11. SME Approval

**Version 1.0 feedback received:**
> "Architecture needs to be better. We can orchestrate the entire flow on AWS
> Glue and also not seeing a requirement of Lambda for the same. Use Glue
> Workflow and maybe promote your Spark code to implement Delta Lake in S3."

**Version 2.0 changes in response:**
- Replaced Lambda with Glue Python Shell job for ingestion
- Replaced Step Functions with Glue Workflow for orchestration
- Replaced plain Parquet with Delta Lake for Silver and Gold layers
- Removed EventBridge (scheduling now inside Glue Workflow)

---
