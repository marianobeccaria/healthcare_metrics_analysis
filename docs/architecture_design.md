# Healthcare Metrics Pipeline — Solution Design Document

**Project:** Healthcare Metrics Pipeline  
**Version:** 2.2 — Split Gold Jobs, EC2 Dashboard, End-to-End Test
**Prepared by:** Mariano Beccaria
**Date:** April 2026  
**Status:** SME Approved — Pipeline Built, Tested, and Live

---

## Revision History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | April 2026 | Initial draft — Lambda + Step Functions + Parquet |
| 2.0 | April 2026 | Revised per SME feedback — Glue Workflow + Delta Lake |
| 2.1 | April 2026 | Updated with actual implementation results and CDK deployment |
| 2.2 | April 2026 | Split Gold into two jobs per SME feedback; EC2 dashboard; end-to-end test passed |

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

**Changes made in v2.1:**
- Corrected encoding from latin-1 to ISO-8859-1 (Spark Java charset name)
- Added CDK infrastructure deployment section
- Added actual implementation and test results
- Noted quality_correlations Gold table as planned (not yet built)

**Changes made in v2.2:**
- Split glue_silver_to_gold.py into two separate jobs per SME feedback:
  glue_silver_to_facility_summary.py writes the facility_summary Gold table
  glue_silver_to_staffing_metrics.py writes the staffing_metrics Gold table
- Both Gold jobs now run in parallel after Bronze to Silver succeeds
- Each Gold job has its own CloudWatch log group for independent debugging
- Added EC2 t3.small instance for Streamlit dashboard hosting (CDK managed)
- End-to-end Glue Workflow test passed — 4/4 actions succeeded
- Dashboard live and accessible via EC2 public IP on port 8501

---

## 1. Summary

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

**Key data quality findings from Step 2 Early Data Analysis:**
- Zero null values across all 33 columns in the PBJ file
- Zero duplicate PROVNUM + WorkDate combinations
- 99.9% join match rate between PBJ and ProviderInfo (17 unmatched CCNs)
- Encoding: all CMS files require latin-1 encoding (Windows-1252 characters in facility names)
- Edge case: rows with MDScensus < 10 indicate reopening facilities and must be excluded from ratio calculations
- Headline finding: only 24.5% of facility-days meet CMS minimum staffing thresholds

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
|          v  (Glue Trigger — SUCCEEDED)            |
|  Job 2: Glue Spark                                |
|  S3 Bronze -> S3 Silver (Delta Lake)              |
|          |              |                         |
|          v              v  (parallel triggers)    |
|  Job 3: Glue Spark   Job 4: Glue Spark            |
|  Silver ->            Silver ->                   |
|  facility_summary     staffing_metrics            |
|  (Delta Lake)         (Delta Lake)                |
+--------------------------------------------------+
     |
     v
Streamlit Dashboard -- Hosted on EC2 (t3.small)
     reads directly from Gold Delta Lake on S3
```

**Orchestration:** AWS Glue Workflow with Glue Triggers chaining all three jobs (job only starts if previous SUCCEEDED)
**Parallel Gold jobs:** Both Gold jobs trigger simultaneously after Bronze to Silver SUCCEEDED
**Infrastructure as Code:** AWS CDK (Python) — full stack deployable with cdk deploy  
**Monitoring:** AWS CloudWatch for Glue job logs, errors, and pipeline alerts

---

## 4. AWS Services — Selection and Rationale

| Service | Role | Why this service |
|---------|------|-----------------|
| **AWS CDK (Python)** | Infrastructure as code | Entire stack in Python — repeatable, version controlled, destroyable with one command |
| **AWS Glue Workflow** | Full pipeline orchestration | Single service for scheduling, job chaining, dependency management |
| **Glue Python Shell job** | Incremental ingestion from Google Drive | Lightweight Python — no Spark cluster needed for file download |
| **Glue Spark job (x3)** | Bronze to Silver; Silver to two Gold tables | Distributed PySpark handles 1.3M+ rows; native Delta Lake support |
| **Amazon S3** | Data lake storage (all three layers) | Cost-effective, durable, integrates natively with Glue |
| **Delta Lake on S3** | Table format for Silver and Gold | ACID transactions, time travel, incremental MERGE |
| **EC2 (t3.small)** | Streamlit dashboard hosting | Simple always-on deployment; managed via CDK |
| **AWS CloudWatch** | Logging and alerting | One log group per Glue job — independent debugging per table |

**Why separate Glue jobs per Gold table (v2.2):**
Previously a single glue_silver_to_gold.py job wrote both Gold tables. Per SME
feedback this was split into two independent jobs. Benefits: if facility_summary
fails, staffing_metrics still runs and vice versa. Each job has its own CloudWatch
log group making it easy to identify which table caused a failure. Both jobs run
in parallel reducing total pipeline runtime.
 
**Why Glue Workflow over Step Functions + Lambda:**
Glue Workflow provides native job chaining within a single service. Conditional
triggers ensure each job only starts if the previous one succeeded.
 
**Why Delta Lake over plain Parquet:**
Delta Lake MERGE allows new quarterly data to be appended without rewriting
historical data. The _delta_log/ transaction log serves as the incremental
watermark — no separate tracking file needed.
 
**Why EC2 over serverless (Lambda/Fargate) for the dashboard:**
Streamlit requires a persistent server process. EC2 t3.small provides a simple
always-on deployment at low cost (~$15/month) with systemd managing restarts.

---

## 5. CDK Infrastructure Stack

### 5.1 Bronze Layer — Raw Ingestion
 
- Files land in S3 exactly as downloaded from Google Drive
- Original filenames preserved — permanent audit record
- No transformations applied
- Partitioned by: quarter=2024Q2/
- Encoding preserved as-is (ISO-8859-1)
- Format: CSV
### 5.2 Silver Layer — Cleaned, Validated, Delta Lake
 
Glue Spark job (glue_bronze_to_silver.py) applies transformations:
 
| Transformation | Reason |
|----------------|--------|
| Load PROVNUM and CCN as string | Preserve leading zeros (e.g. 015009) |
| Apply ISO-8859-1 encoding | Spark Java charset for Windows-1252 characters |
| Filter MDScensus < 10 | Exclude reopening/edge case facilities |
| Route 17 unmatched CCNs to audit table | Never silently drop data |
| Join PBJ to ProviderInfo (LEFT JOIN) | Add facility context — beds, ratings, ownership |
| Parse WorkDate as date type | Enable time-series analysis |
| Cast hours columns to float | Consistent numeric types |
| Add staffing ratios and CMS tier | Core metrics: CNA/RN/total hrs per patient |
| Write as Delta Lake — partitioned by STATE | ACID, incremental merge, fast state queries |
 
**Silver table confirmed:** 1,325,324 rows, 47 columns, partitioned across 52 states
 
### 5.3 Gold Layer — Two Separate Jobs, Delta Lake
 
Per SME feedback, the Gold layer is now written by two independent Spark jobs
that run in parallel after Bronze to Silver succeeds.
 
**Job 3: glue_silver_to_facility_summary.py**
Writes facility_summary — one row per facility per quarter:
- Average staffing ratios (CNA, RN, LPN, total hrs per patient)
- Bed occupancy rate (MDScensus / certified_beds)
- Days meeting CMS minimums + percentage
- Chronic understaffing flag (below CMS minimum >50% of days)
- Weekend vs weekday staffing gap
- Contracted vs employed hours ratio
- Facility metadata: ownership type, star rating, turnover
**Job 4: glue_silver_to_staffing_metrics.py**
Writes staffing_metrics — one row per facility per day:
- Daily ratios for trend charts and time-series analysis
- CMS compliance flag per day
- Weekend/weekday indicator
- Used for Q4 (staffing trends over time) in the dashboard
**quality_correlations — planned, not yet built:**
- Will join Silver with MDS Quality Measures file
- Correlation: CNA ratio vs pressure ulcer rate
- Correlation: RN ratio vs fall injury rate
**CMS minimum thresholds applied:**
 
| Staff Type | CMS Minimum | Source |
|-----------|-------------|--------|
| CNA | 2.45 hrs/patient/day | CMS 2024 rule |
| RN | 0.55 hrs/patient/day | CMS 2024 rule |
| Total nurse | 3.48 hrs/patient/day | CMS 2024 rule |
 
---
 
## 6. CDK Infrastructure Stack
 
All AWS resources defined in infrastructure/infrastructure/healthcare_stack.py
and deployed via AWS CDK.
 
**Resources deployed:**
 
| Resource | Name | Type |
|----------|------|------|
| IAM Role | healthcare-glue-role | Glue service role + S3 read/write |
| IAM Role | healthcare-dashboard-ec2-role | EC2 role + S3 read |
| S3 Structure | mbeccaria-dea-healthcare-metrics | Bronze/Silver/Gold/audit/scripts folders |
| Glue Job | healthcare-ingestion | Python Shell — Google Drive ingestion |
| Glue Job | healthcare-bronze-to-silver | Spark ETL — Glue 4.0, G.1X, 2 workers |
| Glue Job | healthcare-silver-to-facility-summary | Spark ETL — Glue 4.0, G.1X, 2 workers |
| Glue Job | healthcare-silver-to-staffing-metrics | Spark ETL — Glue 4.0, G.1X, 2 workers |
| Glue Workflow | healthcare-metrics-pipeline | End-to-end orchestration |
| Glue Trigger | healthcare-schedule-trigger | Quarterly cron — starts ingestion |
| Glue Trigger | healthcare-bronze-trigger | Conditional — fires after ingestion SUCCEEDED |
| Glue Trigger | healthcare-facility-trigger | Conditional — fires after Bronze-Silver SUCCEEDED |
| Glue Trigger | healthcare-staffing-trigger | Conditional — fires after Bronze-Silver SUCCEEDED |
| CloudWatch Log Group | /aws-glue/healthcare-ingestion | 30-day retention |
| CloudWatch Log Group | /aws-glue/healthcare-bronze-to-silver | 30-day retention |
| CloudWatch Log Group | /aws-glue/healthcare-silver-to-facility-summary | 30-day retention |
| CloudWatch Log Group | /aws-glue/healthcare-silver-to-staffing-metrics | 30-day retention |
| EC2 Instance | DashboardInstance | t3.small, Amazon Linux 2023 |
| Security Group | DashboardSG | Ports 22 (SSH) and 8501 (Streamlit) |
| Key Pair | healthcare-dashboard-key | RSA key for SSH access |
 
**Deploy command:** cdk deploy
**Teardown command:** cdk destroy
 
---
 
## 7. Implementation Results
 
**Glue jobs tested and confirmed working:**
 
| Job | Status | Key Output |
|-----|--------|-----------|
| healthcare-bronze-to-silver | SUCCEEDED | 1,325,324 rows in Silver Delta Lake |
| healthcare-silver-to-facility-summary | SUCCEEDED | 14,523 facility rows in Gold |
| healthcare-silver-to-staffing-metrics | SUCCEEDED | Daily rows in Gold |
| healthcare-ingestion | Placeholder | Google Drive automation — pending |
 
**End-to-end Glue Workflow test:**
Full workflow run triggered manually with Q3 test data (5,000 rows).
Result: 4/4 actions succeeded — ingestion, Bronze-Silver, facility-summary,
staffing-metrics all ran to completion.
 
**S3 structure confirmed:**
```
mbeccaria-dea-healthcare-metrics/
├── bronze/quarter=2024Q2/          <- 21 CSV files
├── bronze/quarter=2024Q3/          <- Q3 test data (5,000 rows)
├── silver/staffing/                <- Delta Lake, 52 state partitions
│   └── _delta_log/
├── gold/facility_summary/          <- Delta Lake, 14,523 facility rows
│   └── _delta_log/
├── gold/staffing_metrics/          <- Delta Lake, daily rows
│   └── _delta_log/
├── audit/unmatched_ccn/            <- 17 unmatched facilities
└── scripts/                        <- 4 Glue scripts + app.py + ec2_setup.sh
```
 
**Streamlit Dashboard — live on EC2:**
- URL: http://[EC2-PUBLIC-IP]:8501
- Instance type: t3.small (2 vCPU, 2GB RAM)
- Managed by: systemd service (auto-restarts on failure or reboot)
- Data source: reads directly from Gold Delta Lake tables on S3
- Features: 8 KPI cards, 4 charts answering all project questions,
  facility drill-down table with CSV export, sidebar filters
---
 
## 8. Streamlit Dashboard Features
 
The dashboard answers all four project questions:
 
| Question | Chart Type | Data Source |
|----------|-----------|-------------|
| Q1: Staffing vs occupancy rates | Scatter plot | Gold facility_summary |
| Q2: Contracted vs employed hours | Bar chart by state | Gold facility_summary |
| Q3: Avg staffing by state and ownership | Horizontal bar | Gold facility_summary |
| Q4: Staffing trends over time | Line chart | Gold staffing_metrics |
 
**Key findings surfaced by the dashboard:**
- Only 9.1% of facilities consistently meet CMS staffing minimums
- 90.9% are chronically understaffed (below minimum >50% of days)
- Average CNA hours (2.11) fall below CMS minimum (2.45) nationally
- Government facilities outperform all for-profit ownership types
- Weekend staffing is consistently worse than weekday (-0.31 hrs average)
- Alaska, Massachusetts, and Oregon rely most heavily on contracted nurses
---
 
## 9. Incremental Ingestion Design
 
CMS releases new quarterly data approximately every 3 months.
 
**Incremental flow:**
1. Glue Workflow triggers on quarterly schedule
2. Python Shell job connects to Google Drive API
3. Job reads Delta Lake history to find last ingested quarter
4. If new quarter exists: download CSVs to Bronze, trigger transformation
5. If no new quarter: log and exit cleanly
**Delta Lake as watermark:**
The _delta_log/ transaction history replaces the manual last_run.json
watermark from v1.0. The ingestion job queries Delta history to determine
what has already been processed, then merges only new data.
 
---
 
## 10. Key EDA Findings
 
| Finding | Architecture Implication |
|---------|------------------------|
| 1,325,324 rows per quarter | Glue Spark required — pandas too slow at scale |
| Zero nulls, zero duplicates | No repair layer needed in Silver |
| 99.9% join match rate | LEFT JOIN confirmed; 17 unmatched CCNs to audit |
| ISO-8859-1 encoding required | Must be specified in all Glue Spark read operations |
| MDScensus < 10 edge cases | Silver layer filter rule |
| Only 24.5% of days meet CMS minimums | Primary dashboard KPI |
| Texas — entire quarter chronically understaffed | State partitioning enables efficient filtering |
 
---
 
## 11. Questions Answered by This Pipeline
 
| Project Question | Data Required | Status |
|----------------|---------------|--------|
| Staffing vs occupancy rates | PBJ + ProviderInfo (beds) | Live in dashboard |
| Highest contracted hours by state | PBJ emp vs ctr columns | Live in dashboard |
| Avg staffing by state and ownership type | PBJ + ProviderInfo | Live in dashboard |
| Staffing trends over time (Q2 2024) | Gold staffing_metrics | Live in dashboard |
 
---
 
## 12. Risks and Mitigations
 
| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Google Drive API rate limits | Medium | Retry logic in Python Shell ingestion job |
| CMS column name changes between quarters | Low-Medium | Silver schema validation — halt on unexpected columns |
| New quarter data quality issues | Medium | Silver validates before Gold — pipeline stops on failures |
| Gold job failure affecting only one table | Low | Jobs are now independent — one failure does not block the other |
| Dashboard unavailable if EC2 restarts | Low | systemd service auto-restarts Streamlit on reboot |
| AWS costs | Low | All services pay-per-use; cdk destroy removes everything |
 
---
 
## 13. SME Approval
 
**Changes implemented in response:**
- v2.0: Replaced Lambda, Step Functions, EventBridge with Glue Workflow
- v2.0: Implemented Delta Lake on S3 for Silver and Gold
- v2.2: Split Gold layer into two separate independent Glue jobs
- v2.2: Added EC2 dashboard deployment via CDK
- v2.2: End-to-end pipeline test passed with simulated Q3 data
---
