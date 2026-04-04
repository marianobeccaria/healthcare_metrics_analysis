# Healthcare Metrics Pipeline — Solution Design Document

**Project:** Healthcare Metrics Pipeline  
**Version:** 1.0 — Draft for SME Review  
**Prepared by:** Mariano Beccaria 
**Date:** April 2026  
**Status:** Pending SME Approval

---

## 1. Executive Summary

This document describes the proposed AWS data pipeline architecture for the
Healthcare Metrics project. The pipeline ingests CMS nursing home staffing data
from Google Drive, transforms it through a three-layer data lake, calculates
key staffing and quality metrics, and surfaces insights via an interactive
Streamlit dashboard.

The architecture is designed to be incremental (processing only new data each
run), scalable (handling 1.3M+ rows per quarter), and fully auditable (all raw
data preserved in Bronze, all transformations traceable).

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
- Encoding: all CMS files require `latin-1` encoding (Windows-1252 characters in facility names)
- Edge case: rows with MDScensus < 10 indicate reopening facilities and must be excluded from ratio calculations

---

## 3. Architecture Overview

The pipeline follows a **Medallion Architecture** (Bronze -> Silver -> Gold),
fully hosted on AWS, ingesting incrementally from Google Drive.

```
Google Drive
     │
     │  (weekly schedule)
AWS Lambda ──── EventBridge (scheduler)
     │   
     │
S3 Bronze  ──── Raw files, unchanged, latin-1 preserved
     │
     │  AWS Glue (PySpark)
     │
S3 Silver  ──── Cleaned, typed, joined, partitioned by STATE
     │
     │  AWS Glue (PySpark)
     │
S3 Silver  ──── Cleaned, typed, joined, partitioned by STATE
S3 Gold    ──── Metrics calculated, Parquet format
     │
     │
S3 Silver  ──── Cleaned, typed, joined, partitioned by STATE
Amazon Athena ── SQL queries on Gold layer
     │
     │
Streamlit Dashboard ── Hosted on EC2 or ECS
```

**Orchestration:** AWS Step Functions chains Lambda -> Glue Silver -> Glue Gold  
**Monitoring:** AWS CloudWatch for logs, errors, and pipeline alerts

---

## 4. AWS Services — Selection and Rationale

| Service | Role | Why this service |
|---------|------|-----------------|
| **AWS Lambda** | Incremental ingestion from Google Drive | Serverless, no infrastructure to manage, ideal for scheduled file checks |
| **Amazon S3** | Data lake storage (all three layers) | Cost-effective, durable, natively integrates with Glue and Athena |
| **AWS Glue** | Data transformation (PySpark) | Serverless ETL, handles large CSVs, native S3 and Athena integration |
| **Amazon Athena** | SQL querying of Gold layer | Serverless, pay-per-query, no warehouse provisioning needed at this data scale |
| **AWS Step Functions** | Pipeline orchestration | Visual workflow, built-in retry logic, chains Lambda and Glue jobs |
| **Amazon EventBridge** | Scheduled pipeline trigger | Cron-based scheduling, integrates natively with Lambda |
| **AWS CloudWatch** | Logging and alerting | Central log aggregation, pipeline failure alerts |
| **EC2 or ECS** | Streamlit dashboard hosting | ECS preferred for containerized deployment; EC2 for simplicity |

**Why Athena over Redshift:**  
At 1.3M rows per quarter, a full Redshift cluster is overprovisioned and
costly. Athena queries S3 directly with no warehouse to manage, and at this
data volume query performance is sufficient for dashboard use. If data grows
beyond ~50M rows or query latency becomes a concern, Redshift can be
introduced at that stage.

---

## 5. Data Lake Layers

### 5.1 Bronze Layer — Raw Ingestion

- Files land in S3 exactly as downloaded from Google Drive
- Original filenames preserved (e.g. `PBJ_Daily_Nurse_Staffing_Q2_2024.csv`)
- No transformations applied — this is the permanent audit record
- Partitioned by: `quarter=2024Q2/`
- Encoding preserved as-is (latin-1)

### 5.2 Silver Layer — Cleaned and Validated

Glue job applies the following transformations informed by Step 2 EDA:

| Transformation | Reason |
|----------------|--------|
| Load PROVNUM and CCN as string | Preserve leading zeros (e.g. `015009`) |
| Apply `latin-1` encoding | CMS files contain Windows-1252 characters |
| Filter MDScensus < 10 | Exclude reopening/edge case facilities |
| Route 17 unmatched CCNs to audit table | Preserve data, flag for investigation |
| Join PBJ to ProviderInfo (LEFT JOIN) | Retain all staffing records, add facility context |
| Parse WorkDate as date type | Enable time-series analysis |
| Convert hours columns to float | Ensure consistent numeric types |
| Convert to Parquet format | 5-10x compression vs CSV, faster Athena queries |
| Partition by STATE | Optimise state-level dashboard queries |

### 5.3 Gold Layer — Metrics

Glue job calculates the following metrics, stored as aggregated Parquet tables:

**Staffing ratio metrics (daily, per facility):**
- `CNA_hrs_per_patient` = Hrs_CNA / MDScensus
- `RN_hrs_per_patient` = Hrs_RN / MDScensus
- `total_hrs_per_patient` = (Hrs_RN + Hrs_LPN + Hrs_CNA) / MDScensus
- `staffing_tier` = categorised against CMS thresholds (see below)

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

CMS releases new quarterly data approximately every 3 months. The pipeline
must process only new quarters, not re-process existing data.

**Watermark mechanism:**
1. A `last_run.json` file in S3 stores the last successfully ingested quarter
2. Lambda reads this file on each scheduled run
3. Lambda checks Google Drive for quarters newer than the watermark
4. If a new quarter exists: download files → update watermark → trigger Glue
5. If no new quarter: log and exit cleanly

**Watermark file format:**
```json
{
  "last_ingested_quarter": "2024Q2",
  "last_run_timestamp": "2024-10-15T08:00:00Z",
  "files_ingested": 16,
  "status": "success"
}
```

---

## 7. Key Data Findings from Step 2 (EDA)

These findings directly inform architecture decisions:

| Finding | Architecture Implication |
|---------|------------------------|
| 1,325,324 rows per quarter | Glue (PySpark) required — pandas would be too slow at scale |
| Zero nulls, zero duplicates | No repair/deduplication layer needed in Silver |
| 99.9% join match rate | LEFT JOIN confirmed; 17 unmatched CCNs → audit table |
| latin-1 encoding required | Must be specified in all Glue read operations |
| MDScensus < 10 edge cases | Silver layer filter rule |
| Only 24.5% of days meet CMS minimums | Primary dashboard KPI |
| Texas facilities — entire quarter understaffed | State-level partitioning enables efficient filtering |
| Length of stay not in PBJ | Medicare Claims QM file required for question #4 |

---

## 8. Questions Answered by This Pipeline

| Project Question | Data Required | Available |
|----------------|---------------|-----------|
| Staffing vs occupancy rates | PBJ + ProviderInfo (beds) | Yes |
| Highest overtime hours | PBJ (emp vs ctr ratio) | Yes |
| Avg staffing by state and type | PBJ + ProviderInfo (ownership) | Yes |
| Length of stay trends | Medicare Claims QM file | Partial — file available but not yet joined |

---

## 9. Risks and Mitigations

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Google Drive API rate limits | Medium | Add retry logic and exponential backoff in Lambda |
| CMS changes column names between quarters | Low-Medium | Silver layer schema validation job alerts on unexpected columns |
| New quarter data quality issues | Medium | Silver layer validation runs before Gold — pipeline halts on failures |
| Streamlit dashboard slow on large queries | Low | Gold layer pre-aggregated; Athena queries on Parquet partitions are fast |
| AWS costs exceed budget | Low | Glue and Athena are pay-per-use; Lambda is near-free at this trigger frequency |

---

## 10. SME Approval

This architecture is presented for review and approval before pipeline
construction begins (Step 4).

**Pending decisions for SME input:**
1. Confirm Athena vs Redshift for the Gold query layer
2. Confirm EC2 vs ECS for Streamlit hosting
3. Approve the 17 unmatched CCN routing strategy (audit table vs exclusion)
4. Confirm whether Medicare Claims QM file should be included in initial scope

---
