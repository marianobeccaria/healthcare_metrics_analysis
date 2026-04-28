# ============================================================
# Glue Job: Silver → Gold (Staffing Metrics)
# Healthcare Metrics Pipeline
# ============================================================
# WHAT THIS JOB DOES:
#   1. Reads cleaned Silver Delta Lake table
#   2. Calculates daily staffing metrics vs CMS thresholds
#   3. Writes ONE ROW PER FACILITY PER DAY to staffing_metrics
#      Gold Delta Lake table
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

BUCKET_NAME         = args["BUCKET_NAME"]
SILVER_PATH         = args["SILVER_PATH"]
GOLD_PATH           = args["GOLD_PATH"]
QUARTER             = args["QUARTER"]
GOLD_STAFFING_PATH  = GOLD_PATH + "staffing_metrics/"

logger.info(f"Starting Silver to Staffing Metrics job")
logger.info(f"  SILVER_PATH        : {SILVER_PATH}")
logger.info(f"  GOLD_STAFFING_PATH : {GOLD_STAFFING_PATH}")
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
# Filter to current quarter only.
logger.info(f"Reading Silver Delta table for quarter: {QUARTER}")

df_silver = spark.read \
    .format("delta") \
    .load(SILVER_PATH) \
    .filter(F.col("quarter") == QUARTER)

silver_count = df_silver.count()
logger.info(f"Silver rows loaded for {QUARTER}: {silver_count:,}")

# ── Step 4: Calculate daily staffing metrics ──────────────────
# Row-level calculations — one output row per input row.
# No aggregation here — this table stays at daily granularity
# for trend charts and time-series analysis in the dashboard.
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

# ── Step 5: Select columns for Gold staffing metrics table ────
# Only carry dashboard-relevant columns into Gold.
# Keeping Gold lean makes Athena queries faster.
df_staffing_metrics = df_daily.select(
    "PROVNUM", "PROVNAME", "STATE", "WorkDate", "quarter",
    "MDScensus", "CNA_hrs_per_patient", "RN_hrs_per_patient",
    "LPN_hrs_per_patient", "total_hrs_per_patient",
    "bed_occupancy_rate", "contracted_rn_ratio",
    "meets_cms_minimums", "staffing_tier", "is_weekend",
    "day_of_week", "ownership_type", "overall_rating",
    "certified_beds",
)

logger.info(f"Staffing metrics columns: {len(df_staffing_metrics.columns)}")

# ── Step 6: Write Gold — staffing metrics Delta Lake table ────
# One row per facility per day — used for trend charts.
# Uses whenNotMatchedInsertAll only — daily rows never change
# once written so no update needed.
logger.info("Writing Gold staffing metrics Delta Lake table...")

delta_exists = DeltaTable.isDeltaTable(spark, GOLD_STAFFING_PATH)

if not delta_exists:
    logger.info("Gold staffing table does not exist — creating fresh...")
    df_staffing_metrics.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("STATE") \
        .save(GOLD_STAFFING_PATH)
    logger.info(f"Gold staffing table created at: {GOLD_STAFFING_PATH}")
else:
    logger.info("Gold staffing table exists — merging new rows...")
    delta_table = DeltaTable.forPath(spark, GOLD_STAFFING_PATH)
    delta_table.alias("target").merge(
        df_staffing_metrics.alias("source"),
        "target.PROVNUM = source.PROVNUM AND "
        "target.WorkDate = source.WorkDate"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()
    logger.info("Gold staffing metrics merge completed")

# ── Step 7: Validate ─────────────────────────────────────────
logger.info("Validating Gold staffing metrics table...")

df_validate = spark.read.format("delta").load(GOLD_STAFFING_PATH)
row_count = df_validate.count()

logger.info(f"Gold staffing rows : {row_count:,}")
logger.info(f"Gold staffing cols : {len(df_validate.columns)}")

# show CMS compliance rate from daily data
meets = df_validate.filter(F.col("meets_cms_minimums") == True).count()
pct   = round(meets / row_count * 100, 1)
logger.info(
    f"Daily rows meeting CMS minimums: {meets:,}/{row_count:,} ({pct}%)"
)

# ── Step 8: Commit ────────────────────────────────────────────
logger.info("Silver to Staffing Metrics job completed successfully")
job.commit()
