# Healthcare Metrics Pipeline — Solution Design Document

**Project:** Healthcare Metrics Pipeline  
**Version:** 2.3 — Google Drive API Ingestion  
**Prepared by:** [Your Name]  
**Date:** May 2026  
**Status:** SME Approved — Pipeline Built, Tested, and Live

---

## Revision History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | April 2026 | Initial draft — Lambda + Step Functions + Parquet |
| 2.0 | April 2026 | Revised per SME feedback — Glue Workflow + Delta Lake |
| 2.1 | April 2026 | Implementation results, CDK deployment added |
| 2.2 | April 2026 | Split Gold into two jobs per SME feedback; EC2 dashboard; end-to-end test passed |
| 2.3 | May 2026 | Implemented Google Drive API ingestion with service account authentication and incremental load logic |

**SME Feedback (v1.0 → v2.0):**
> "There are good analysis reports on your repository but the architecture needs
> to be better. We can orchestrate the entire flow on AWS Glue and also not
> seeing a requirement of Lambda for the same. Use Glue Workflow and maybe
> promote your Spark code to implement Delta Lake in S3."

**SME Feedback (v2.1 → v2.2):**
> "This looks really nice overall. One suggestion I would recommend is using
> separate Glue jobs per table since it will make debugging much easier."

**SME Feedback (v2.2 → v2.3):**
> "How are you handling incremental loads in your current setup? Since files
> are being manually downloaded, are you tracking new or updated files using
> something like load_date, filename or any watermark logic?"

**Changes made in v2.0:**
- Removed AWS Lambda — ingestion handled by Glue Python Shell job
- Removed AWS Step Functions — orchestration handled by Glue Workflow
- Removed Amazon EventBridge — scheduling inside Glue Workflow trigger
- Added Delta Lake format to Silver and Gold layers
- Added Glue Workflow as the single orchestration layer

**Changes made in v2.1:**
- Corrected encoding from latin-1 to ISO-8859-1 (Spark Java charset)
- Added CDK infrastructure deployment section
- Added actual implementation and test results

**Changes made in v2.2:**
- Split glue_silver_to_gold.py into two separate jobs per SME feedback:
  glue_silver_to_facility_summary.py writes the facility_summary Gold table
  glue_silver_to_staffing_metrics.py writes the staffing_metrics Gold table
- Both Gold jobs now run in parallel after Bronze to Silver succeeds
- Each Gold job has its own CloudWatch log group for independent debugging
- Added EC2 t3.small instance for Streamlit dashboard hosting (CDK managed)
- End-to-end Glue Workflow test passed — 4/4 actions succeeded

**Changes made in v2.3:**
- Implemented real Google Drive API connection in glue_ingestion.py
- Authentication via Google service account — JSON key stored in AWS Secrets Manager
- Incremental ingestion logic — compares available Drive quarters against
  existing Bronze S3 partitions and downloads only new quarters
- Google Drive folder ID moved to environment variable (HEALTHCARE_DRIVE_FOLDER_ID)
  so it can be changed without modifying code
- Glue IAM role updated with Secrets Manager GetSecretValue permission
- Ingestion job tested end-to-end — authenticated, listed 20 CSV files,
  detected no new quarters, exited cleanly

---

## 1. Executive Summary

This document describes the AWS data pipeline architecture for the Healthcare
Metrics project. The pipeline ingests CMS nursing home staffing data from Google
Drive, transforms it through a three-layer Delta Lake on S3, calculates key
staffing and quality metrics, and surfaces insights via an interactive Streamlit
dashboard deployed on EC2.

The architecture is unified entirely within AWS Glue — a single Glue Workflow
orchestrates ingestion, transformation, and metric calculation. Delta Lake on S3
provides ACID transactions, time travel, and native incremental merge capability.
All AWS infrastructure is defined and deployed as code using AWS CDK (Python),
including the EC2 dashboard instance.

---

## 2. Data Sources

| File | Description | Rows (Q2 2024) | Join Key |
|------|-------------|---------------|----------|
| PBJ_Daily_Nurse_Staffing_Q2_2024.csv | Daily staffing hours per facility | 1,325,324 | PROVNUM |
| NH_ProviderInfo_*.csv | Facility details, ratings, bed counts | 14,814 | CCN |
| NH_QualityMsr_MDS_*.csv | MDS quality measures per facility | ~300,000 | CCN |
| NH_QualityMsr_Claims_*.csv | Medicare claims quality measures | ~150,000 | CCN |
| FY_2024_SNF_VBP_*.csv | Value-based purchasing scores | ~15,000 | CCN |
| NH_HealthCitations_*.csv | Health inspection deficiencies | ~200,000 | CCN |
| NH_Penalties_*.csv | Fines and payment denials | ~50,000 | CCN |
| (+ 13 additional supporting files) | Ownership, surveys, COVID vax | varies | CCN |

**Key EDA findings:**
- Zero null values across all 33 columns in the PBJ file
- Zero duplicate PROVNUM + WorkDate combinations
- 99.9% join match rate between PBJ and ProviderInfo (17 unmatched CCNs)
- Encoding: all CMS files require ISO-8859-1 (Spark Java charset)
- Only 24.5% of facility-days meet CMS minimum staffing thresholds

---

## 3. Architecture Overview

```
Google Drive (source data folder)
     |
     |  Glue Workflow — quarterly schedule trigger
     v
+--------------------------------------------------+
|  Job 1: Glue Python Shell — ingestion            |
|  - Authenticates via service account             |
|  - Lists CSV files in Drive folder               |
|  - Compares against S3 Bronze partitions         |
|  - Downloads new quarters only                   |
|          |                                        |
|          v  Trigger: ingestion SUCCEEDED          |
|  Job 2: Glue Spark — Bronze to Silver            |
|          |                                        |
|          v  Parallel triggers: both fire after   |
|             Bronze to Silver SUCCEEDED           |
|  Job 3: Glue Spark      Job 4: Glue Spark        |
|  Silver →               Silver →                 |
|  facility_summary       staffing_metrics          |
|  (Delta Lake)           (Delta Lake)              |
+--------------------------------------------------+
     |
     v
Streamlit Dashboard — EC2 t3.small — port 8501
(reads directly from Gold Delta Lake on S3)
```

---

## 4. AWS Services — Selection and Rationale

| Service | Role | Why this service |
|---------|------|-----------------|
| **AWS CDK (Python)** | Infrastructure as code | Full stack in Python — repeatable, version controlled, destroyable with one command |
| **AWS Glue Workflow** | Full pipeline orchestration | Single service for scheduling, job chaining, dependency management |
| **Glue Python Shell job** | Incremental ingestion from Google Drive | Lightweight Python — no Spark cluster needed for file download |
| **Glue Spark job (x3)** | Bronze to Silver; two Gold tables | Distributed PySpark handles 1.3M+ rows; native Delta Lake support |
| **Amazon S3** | Data lake storage (all three layers) | Cost-effective, durable, integrates natively with Glue |
| **Delta Lake on S3** | Table format for Silver and Gold | ACID transactions, time travel, incremental MERGE |
| **AWS Secrets Manager** | Google Drive service account credentials | Secure credential storage — never in code or Git |
| **EC2 (t3.small)** | Streamlit dashboard hosting | Simple always-on deployment; managed via CDK |
| **AWS CloudWatch** | Logging and alerting | One log group per Glue job — independent debugging per table |

**Why Secrets Manager for Google Drive credentials:**
The Google service account JSON key grants access to Google Drive. Storing it
in Secrets Manager means the key never touches the codebase or Git history.
The Glue job retrieves it at runtime using `boto3.secretsmanager` and writes
it to a temporary file that is deleted after authentication. The Glue IAM role
has `secretsmanager:GetSecretValue` permission scoped to the specific secret ARN.

**Why separate Glue jobs per Gold table (v2.2):**
Previously a single glue_silver_to_gold.py job wrote both Gold tables. Per SME
feedback this was split into two independent jobs. If facility_summary fails,
staffing_metrics still runs and vice versa. Each job has its own CloudWatch
log group making it easy to identify which table caused a failure.

---

## 5. Data Lake Layers

### 5.1 Bronze Layer — Raw Ingestion

- Files land in S3 exactly as downloaded from Google Drive
- Partitioned by quarter: `bronze/quarter=2024Q2/`
- No transformations applied — permanent audit record
- Format: CSV, ISO-8859-1 encoding preserved

### 5.2 Silver Layer — Cleaned, Validated, Delta Lake

Key transformations applied by glue_bronze_to_silver.py:

| Transformation | Reason |
|----------------|--------|
| Load PROVNUM and CCN as string | Preserve leading zeros |
| Apply ISO-8859-1 encoding | Spark Java charset for Windows-1252 characters |
| Filter MDScensus < 10 | Exclude reopening/edge case facilities |
| Route 17 unmatched CCNs to audit table | Never silently drop data |
| LEFT JOIN PBJ to ProviderInfo | Add facility context |
| Parse WorkDate as date type | Enable time-series analysis |
| Add staffing ratios and CMS tier | Core metrics: CNA/RN/total hrs per patient |
| Write as Delta Lake — partitioned by STATE | ACID, incremental merge |

**Silver confirmed:** 1,325,324 rows, 47 columns, 52 state partitions

### 5.3 Gold Layer — Two Separate Jobs, Delta Lake

**Job 3: glue_silver_to_facility_summary.py**
One row per facility per quarter. Aggregates:
- Average staffing ratios (CNA, RN, LPN, total hrs per patient)
- Bed occupancy rate (MDScensus / certified_beds)
- Days meeting CMS minimums + percentage
- Chronic understaffing flag (below CMS minimum >50% of days)
- Weekend vs weekday staffing gap
- Contracted vs employed hours ratio

**Job 4: glue_silver_to_staffing_metrics.py**
One row per facility per day. Used for:
- Trend charts and time-series analysis in dashboard
- CMS compliance flag per day
- Weekend/weekday indicator

**CMS minimum thresholds:**

| Staff Type | CMS Minimum |
|-----------|-------------|
| CNA | 2.45 hrs/patient/day |
| RN | 0.55 hrs/patient/day |
| Total nurse | 3.48 hrs/patient/day |

---

## 6. CDK Infrastructure Stack

All AWS resources defined in `infrastructure/infrastructure/healthcare_stack.py`.

**Resources deployed:**

| Resource | Name |
|----------|------|
| IAM Role | healthcare-glue-role (S3 read/write + Secrets Manager) |
| IAM Role | healthcare-dashboard-ec2-role (S3 read) |
| S3 Structure | Bronze/Silver/Gold/audit/scripts folders |
| Glue Job | healthcare-ingestion (Python Shell) |
| Glue Job | healthcare-bronze-to-silver (Spark, G.1X, 2 workers) |
| Glue Job | healthcare-silver-to-facility-summary (Spark, G.1X, 2 workers) |
| Glue Job | healthcare-silver-to-staffing-metrics (Spark, G.1X, 2 workers) |
| Glue Workflow | healthcare-metrics-pipeline |
| Glue Trigger | healthcare-schedule-trigger (quarterly cron) |
| Glue Trigger | healthcare-bronze-trigger (conditional) |
| Glue Trigger | healthcare-facility-trigger (conditional, parallel) |
| Glue Trigger | healthcare-staffing-trigger (conditional, parallel) |
| CloudWatch Log Group | /aws-glue/healthcare-ingestion (30 days) |
| CloudWatch Log Group | /aws-glue/healthcare-bronze-to-silver (30 days) |
| CloudWatch Log Group | /aws-glue/healthcare-silver-to-facility-summary (30 days) |
| CloudWatch Log Group | /aws-glue/healthcare-silver-to-staffing-metrics (30 days) |
| EC2 Instance | t3.small, Amazon Linux 2023 |
| Security Group | Ports 22 (SSH) and 8501 (Streamlit) |
| Key Pair | healthcare-dashboard-key (RSA) |

**Deploy:** `cdk deploy`  
**Teardown:** `cdk destroy` (S3 bucket and data retained)

---

## 7. Incremental Ingestion Design

### Problem
CMS releases new quarterly data approximately every 3 months. The pipeline needs
to detect and download only new quarters — not re-download everything each run.

### Solution — Three-layer incremental logic

**Layer 1 — Drive folder scan:**
The ingestion job queries the Google Drive folder for all CSV files. It identifies
quarter strings from PBJ filenames using regex (e.g. `Q2_2024` → `2024Q2`).

**Layer 2 — S3 Bronze comparison:**
The job lists existing S3 Bronze partitions (e.g. `bronze/quarter=2024Q2/`).
Any quarter found on Drive but not in Bronze is marked for download.

**Layer 3 — Delta Lake MERGE:**
The Silver and Gold jobs use Delta Lake MERGE operations. New rows are only
inserted when `PROVNUM + WorkDate` don't already exist in the target table.
Running the same job twice on the same quarter produces no duplicates.

```
Drive folder                S3 Bronze partitions
2024Q2 ←───────────────── already exists → skip
2024Q3 ←───────────────── not found → download
2024Q4 ←───────────────── not found → download
```

### Watermark
The `_delta_log/` transaction history in Silver and Gold serves as an implicit
watermark — queryable to determine what quarters have already been processed
without maintaining a separate tracking file.

---

## 8. Google Drive API Ingestion — Technical Details

**Authentication flow:**
1. Glue job starts and calls AWS Secrets Manager to retrieve the service account JSON key
2. Key is written to a temporary file (required by Google Auth library)
3. `google.oauth2.service_account.Credentials` authenticates using the key
4. `googleapiclient.discovery.build("drive", "v3")` creates the Drive API client
5. Temporary credentials file is deleted after authentication

**Why service account over OAuth:**
OAuth requires interactive browser-based consent — not suitable for automated
pipelines. Service accounts authenticate programmatically without user interaction
and can be scoped to read-only Drive access.

**Why Secrets Manager over environment variables:**
Environment variables in Glue job arguments are visible in CloudWatch logs and
the AWS console. Secrets Manager encrypts the credential at rest and in transit,
and access is controlled by IAM policy scoped to the specific secret ARN.

**Key configuration:**
```
Secret name:     healthcare/google-drive-credentials
Secret content:  Google service account JSON key (full file)
IAM permission:  secretsmanager:GetSecretValue
                 Resource: arn:aws:secretsmanager:us-east-1:*:secret:healthcare/google-drive-credentials-*
Drive folder:    Configured via HEALTHCARE_DRIVE_FOLDER_ID env var in .env
```

**Tested result:**
- Authenticated to Google Drive successfully ✅
- Listed 20 CSV files in the Drive folder ✅
- Detected existing Bronze quarters (2024Q2, 2024Q3) ✅
- Correctly identified no new quarters to download ✅
- Exited cleanly with "No new quarters found" ✅

---

## 9. Implementation Results

| Job | Status | Output |
|-----|--------|--------|
| healthcare-ingestion | ✅ SUCCEEDED | Authenticated, incremental check, clean exit |
| healthcare-bronze-to-silver | ✅ SUCCEEDED | 1,325,324 rows in Silver Delta Lake |
| healthcare-silver-to-facility-summary | ✅ SUCCEEDED | 14,523 facility rows in Gold |
| healthcare-silver-to-staffing-metrics | ✅ SUCCEEDED | Daily rows in Gold |

**End-to-end Glue Workflow test:**
Full workflow triggered with simulated Q3 data — 4/4 actions succeeded.

**S3 structure confirmed:**
```
mbeccaria-dea-healthcare-metrics/
├── bronze/quarter=2024Q2/     <- 21 CSV files
├── bronze/quarter=2024Q3/     <- Q3 test data
├── silver/staffing/           <- Delta Lake, 52 state partitions
├── gold/facility_summary/     <- Delta Lake, 14,523 rows
├── gold/staffing_metrics/     <- Delta Lake, daily rows
└── audit/unmatched_ccn/       <- 17 unmatched facilities
```

---

## 10. Streamlit Dashboard Features

Live at `http://[EC2-PUBLIC-IP]:8501`

| Question | Chart Type | Data Source |
|----------|-----------|-------------|
| Q1: Staffing vs occupancy rates | Scatter plot | Gold facility_summary |
| Q2: Contracted vs employed hours | Bar chart by state | Gold facility_summary |
| Q3: Avg staffing by state and ownership | Horizontal bar | Gold facility_summary |
| Q4: Staffing trends over time | Line chart | Gold staffing_metrics |

**Key findings:**
- Only 9.1% of facilities consistently meet CMS staffing minimums
- 90.9% are chronically understaffed
- Average CNA hours (2.11) fall below CMS minimum (2.45) nationally
- Government facilities outperform all for-profit ownership types
- Weekend staffing worse than weekday by -0.31 hrs on average

---

## 11. Risks and Mitigations

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Google Drive API rate limits | Medium | Retry logic; Python Shell job runs quarterly not daily |
| Service account key expiry | Low | Key stored in Secrets Manager — rotate without code changes |
| CMS column name changes | Low-Medium | Silver schema validation halts pipeline on unexpected columns |
| Gold job failure on one table | Low | Independent jobs — one failure does not block the other |
| Dashboard unavailable on EC2 restart | Low | systemd service auto-restarts Streamlit on reboot |

---
