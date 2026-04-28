# ============================================================
# Glue Job: Silver → Gold (Facility Summary)
# Healthcare Metrics Pipeline
# ============================================================
# WHAT THIS JOB DOES:
#   1. Reads cleaned Silver Delta Lake table
#   2. Calculates daily staffing metrics vs CMS thresholds
#   3. Aggregates to ONE ROW PER FACILITY per quarter
#   4. Writes facility_summary Gold Delta Lake table
#
# RUNS ON: AWS Glue 4.0 (Spark 3.3, Python 3.10)
#
# SME feedback: split Silver→Gold into separate jobs per table
# so each table can be debugged, rerun, and monitored
# independently via CloudWatch.
# ============================================================

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from delta.tables import DeltaTable

# ── Logging setup ─────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ── Step 1: Read job arguments ────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "BUCKET_NAME",
    "SILVER_PATH",
    "GOLD_PATH",
    "QUARTER",
])

BUCKET_NAME        = args["BUCKET_NAME"]
SILVER_PATH        = args["SILVER_PATH"]
GOLD_PATH          = args["GOLD_PATH"]
QUARTER            = args["QUARTER"]
GOLD_FACILITY_PATH = GOLD_PATH + "facility_summary/"

logger.info(f"Starting Silver to Facility Summary job")
logger.info(f"  SILVER_PATH        : {SILVER_PATH}")
logger.info(f"  GOLD_FACILITY_PATH : {GOLD_FACILITY_PATH}")
logger.info(f"  QUARTER            : {QUARTER}")

# ── Step 2: Initialize Spark and Glue context ─────────────────
spark = SparkSession.builder \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

glue_context = GlueContext(spark.sparkContext)
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

logger.info("SparkSession initialized successfully")

# ── Step 3: Read Silver Delta Lake table ──────────────────────
# Filter to current quarter only — Gold only processes new data.
# Historical quarters already exist in Gold from previous runs.
logger.info(f"Reading Silver Delta table for quarter: {QUARTER}")

df_silver = spark.read \
    .format("delta") \
    .load(SILVER_PATH) \
    .filter(F.col("quarter") == QUARTER)

silver_count = df_silver.count()
logger.info(f"Silver rows loaded for {QUARTER}: {silver_count:,}")

# ── Step 4: Calculate daily staffing metrics ──────────────────
# Row-level calculations — one output row per input row.
# These become the inputs for the facility-level aggregation.
logger.info("Calculating daily staffing metrics...")

df_daily = df_silver \
    .withColumn(
        "CNA_hrs_per_patient",
        F.round(
            F.when(F.col("MDScensus") > 0,
                   F.col("Hrs_CNA") / F.col("MDScensus"))
             .otherwise(F.lit(None)), 4)
    ) \
    .withColumn(
        "RN_hrs_per_patient",
        F.round(
            F.when(F.col("MDScensus") > 0,
                   F.col("Hrs_RN") / F.col("MDScensus"))
             .otherwise(F.lit(None)), 4)
    ) \
    .withColumn(
        "LPN_hrs_per_patient",
        F.round(
            F.when(F.col("MDScensus") > 0,
                   F.col("Hrs_LPN") / F.col("MDScensus"))
             .otherwise(F.lit(None)), 4)
    ) \
    .withColumn(
        "total_hrs_per_patient",
        F.round(
            F.when(F.col("MDScensus") > 0,
                   (F.col("Hrs_RN") + F.col("Hrs_LPN") + F.col("Hrs_CNA"))
                   / F.col("MDScensus"))
             .otherwise(F.lit(None)), 4)
    ) \
    .withColumn(
        "bed_occupancy_rate",
        F.round(
            F.when(
                (F.col("certified_beds").isNotNull()) &
                (F.col("certified_beds") > 0),
                F.col("MDScensus") / F.col("certified_beds")
            ).otherwise(F.lit(None)), 4)
    ) \
    .withColumn(
        "contracted_rn_ratio",
        F.round(
            F.when(F.col("Hrs_RN") > 0,
                   F.col("Hrs_RN_ctr") / F.col("Hrs_RN"))
             .otherwise(F.lit(0.0)), 4)
    ) \
    .withColumn(
        # CMS compliance flag — meets ALL three minimums?
        "meets_cms_minimums",
        (F.col("CNA_hrs_per_patient") >= 2.45) &
        (F.col("RN_hrs_per_patient") >= 0.55) &
        (F.col("total_hrs_per_patient") >= 3.48)
    ) \
    .withColumn(
        "day_of_week",
        F.dayofweek(F.col("WorkDate"))
    ) \
    .withColumn(
        "is_weekend",
        F.col("day_of_week").isin([1, 7])
    )

logger.info("Daily metrics calculated")

# ── Step 5: Aggregate to facility level ───────────────────────
# Collapse all daily rows into ONE ROW per facility per quarter.
# This is the core output of this job.
logger.info("Aggregating to facility level...")

df_facility = df_daily.groupBy(
    "PROVNUM", "PROVNAME", "STATE", "CITY",
    "ownership_type", "provider_type",
    "certified_beds", "overall_rating",
    "staffing_rating", "nursing_turnover", "quarter"
).agg(
    F.round(F.mean("MDScensus"), 1).alias("avg_daily_census"),
    F.round(F.mean("bed_occupancy_rate"), 4).alias("avg_bed_occupancy_rate"),
    F.round(F.mean("CNA_hrs_per_patient"), 4).alias("avg_CNA_hrs_per_patient"),
    F.round(F.mean("RN_hrs_per_patient"), 4).alias("avg_RN_hrs_per_patient"),
    F.round(F.mean("LPN_hrs_per_patient"), 4).alias("avg_LPN_hrs_per_patient"),
    F.round(F.mean("total_hrs_per_patient"), 4).alias("avg_total_hrs_per_patient"),
    F.round(F.sum("Hrs_RN"), 1).alias("total_RN_hours"),
    F.round(F.sum("Hrs_LPN"), 1).alias("total_LPN_hours"),
    F.round(F.sum("Hrs_CNA"), 1).alias("total_CNA_hours"),
    F.round(F.mean("contracted_rn_ratio"), 4).alias("avg_contracted_rn_ratio"),
    F.round(F.sum("Hrs_RN_ctr"), 1).alias("total_contracted_RN_hours"),
    F.round(F.sum("Hrs_RN_emp"), 1).alias("total_employed_RN_hours"),
    F.sum(F.col("meets_cms_minimums").cast("int")).alias("days_meeting_cms_minimum"),
    F.count("WorkDate").alias("total_days_in_quarter"),
    F.round(
        F.mean(F.when(F.col("is_weekend"), F.col("total_hrs_per_patient"))), 4
    ).alias("avg_weekend_hrs_per_patient"),
    F.round(
        F.mean(F.when(~F.col("is_weekend"), F.col("total_hrs_per_patient"))), 4
    ).alias("avg_weekday_hrs_per_patient"),
)

# ── Step 6: Add derived facility-level columns ────────────────
# Must come AFTER aggregation — these depend on aggregated values
logger.info("Adding derived facility-level columns...")

df_facility = df_facility \
    .withColumn(
        "pct_days_meeting_cms",
        F.round(
            F.col("days_meeting_cms_minimum") /
            F.col("total_days_in_quarter") * 100, 1)
    ) \
    .withColumn(
        "chronically_understaffed",
        F.col("pct_days_meeting_cms") < 50.0
    ) \
    .withColumn(
        "weekend_staffing_gap",
        F.round(
            F.col("avg_weekend_hrs_per_patient") -
            F.col("avg_weekday_hrs_per_patient"), 4)
    )

facility_count = df_facility.count()
logger.info(f"Facility summary rows: {facility_count:,}")

# ── Step 7: Write Gold — facility summary Delta Lake table ────
# One row per facility per quarter.
# Uses whenMatchedUpdateAll so reruns recalculate correctly.
logger.info("Writing Gold facility summary Delta Lake table...")

delta_exists = DeltaTable.isDeltaTable(spark, GOLD_FACILITY_PATH)

if not delta_exists:
    logger.info("Gold facility table does not exist — creating fresh...")
    df_facility.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("STATE") \
        .save(GOLD_FACILITY_PATH)
    logger.info(f"Gold facility table created at: {GOLD_FACILITY_PATH}")
else:
    logger.info("Gold facility table exists — merging...")
    delta_table = DeltaTable.forPath(spark, GOLD_FACILITY_PATH)
    delta_table.alias("target").merge(
        df_facility.alias("source"),
        "target.PROVNUM = source.PROVNUM AND "
        "target.quarter = source.quarter"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
    logger.info("Gold facility table merge completed")

# ── Step 8: Validate ─────────────────────────────────────────
logger.info("Validating Gold facility summary table...")

df_validate = spark.read.format("delta").load(GOLD_FACILITY_PATH)
total     = df_validate.count()
compliant = df_validate.filter(
    F.col("chronically_understaffed") == False
).count()
pct = round(compliant / total * 100, 1)

logger.info(f"Gold facility rows           : {total:,}")
logger.info(
    f"Facilities meeting CMS >50%  : {compliant:,}/{total:,} ({pct}%)"
)

logger.info("Top 10 chronically understaffed facilities:")
df_validate \
    .filter(F.col("chronically_understaffed") == True) \
    .orderBy("pct_days_meeting_cms") \
    .select("PROVNAME", "STATE", "avg_CNA_hrs_per_patient",
            "pct_days_meeting_cms", "avg_daily_census") \
    .show(10, truncate=False)

# ── Step 9: Commit ────────────────────────────────────────────
logger.info("Silver to Facility Summary job completed successfully")
job.commit()
