# ============================================================
# Glue Job: Silver → Gold
# Healthcare Metrics Pipeline
# ============================================================
# WHAT THIS JOB DOES:
#   1. Reads cleaned Silver Delta Lake table
#   2. Calculates staffing metrics vs CMS thresholds
#   3. Aggregates to facility level (per quarter)
#   4. Calculates quality correlations (MDS join)
#   5. Writes Gold Delta Lake tables for dashboard
#
# RUNS ON: AWS Glue 4.0 (Spark 3.3, Python 3.10)
# ============================================================

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
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

BUCKET_NAME = args["BUCKET_NAME"]
SILVER_PATH = args["SILVER_PATH"]
GOLD_PATH   = args["GOLD_PATH"]
QUARTER     = args["QUARTER"]

# Gold layer output paths — one Delta table per metric group
GOLD_STAFFING_PATH     = GOLD_PATH + "staffing_metrics/"
GOLD_FACILITY_PATH     = GOLD_PATH + "facility_summary/"
GOLD_UNDERSTAFFED_PATH = GOLD_PATH + "quality_correlations/"

logger.info(f"Starting Silver to Gold job")
logger.info(f"  SILVER_PATH : {SILVER_PATH}")
logger.info(f"  GOLD_PATH   : {GOLD_PATH}")
logger.info(f"  QUARTER     : {QUARTER}")

# ── Step 2: Initialize Spark and Glue context ─────────────────
# Identical to Bronze → Silver — Delta Lake config required
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
# Reading Delta is simpler than reading CSV —
# schema is already enforced, types are correct,
# no encoding issues, no inferSchema needed.
#
# We filter to current quarter only so Gold tables
# always reflect the latest quarter being processed.
# Historical quarters are already in Gold from previous runs.
logger.info(f"Reading Silver Delta table for quarter: {QUARTER}")

df_silver = spark.read \
    .format("delta") \
    .load(SILVER_PATH) \
    .filter(F.col("quarter") == QUARTER)

silver_count = df_silver.count()
logger.info(f"Silver rows loaded for {QUARTER}: {silver_count:,}")

logger.info(f"Silver columns: {df_silver.columns}")

# ── Step 4: Calculate daily staffing metrics per facility ─────
# These are row-level calculations — one output row per input row.
# Same ratios we calculated in Bronze->Silver but now we also
# add the CMS compliance flag and bed occupancy rate.
#
# Round to 4 decimal places for clean storage.
# F.round() is the PySpark equivalent of Python's round()
logger.info("Calculating daily staffing metrics...")

df_daily = df_silver \
    .withColumn(
        "CNA_hrs_per_patient",
        F.round(
            F.when(F.col("MDScensus") > 0,
                   F.col("Hrs_CNA") / F.col("MDScensus"))
             .otherwise(F.lit(None)),
            4
        )
    ) \
    .withColumn(
        "RN_hrs_per_patient",
        F.round(
            F.when(F.col("MDScensus") > 0,
                   F.col("Hrs_RN") / F.col("MDScensus"))
             .otherwise(F.lit(None)),
            4
        )
    ) \
    .withColumn(
        "LPN_hrs_per_patient",
        F.round(
            F.when(F.col("MDScensus") > 0,
                   F.col("Hrs_LPN") / F.col("MDScensus"))
             .otherwise(F.lit(None)),
            4
        )
    ) \
    .withColumn(
        "total_hrs_per_patient",
        F.round(
            F.when(F.col("MDScensus") > 0,
                   (F.col("Hrs_RN") + F.col("Hrs_LPN") + F.col("Hrs_CNA"))
                   / F.col("MDScensus"))
             .otherwise(F.lit(None)),
            4
        )
    ) \
    .withColumn(
        # bed occupancy rate — needs certified_beds from ProviderInfo
        # guard against null certified_beds (unmatched facilities)
        "bed_occupancy_rate",
        F.round(
            F.when(
                (F.col("certified_beds").isNotNull()) &
                (F.col("certified_beds") > 0),
                F.col("MDScensus") / F.col("certified_beds")
            ).otherwise(F.lit(None)),
            4
        )
    ) \
    .withColumn(
        # contracted ratio — what % of total RN hours are contracted
        "contracted_rn_ratio",
        F.round(
            F.when(F.col("Hrs_RN") > 0,
                   F.col("Hrs_RN_ctr") / F.col("Hrs_RN"))
             .otherwise(F.lit(0.0)),
            4
        )
    ) \
    .withColumn(
        # CMS compliance flag - meets all three minimums?
        # True only if CNA >= 2.45 AND RN >= 0.55 AND total >= 3.48
        "meets_cms_minimums",
        (F.col("CNA_hrs_per_patient") >= 2.45) &
        (F.col("RN_hrs_per_patient") >= 0.55) &
        (F.col("total_hrs_per_patient") >= 3.48)
    ) \
    .withColumn(
        # day of week - useful for weekend vs weekday analysis
        # F.dayofweek returns 1=Sunday through 7=Saturday
        "day_of_week",
        F.dayofweek(F.col("WorkDate"))
    ) \
    .withColumn(
        "is_weekend",
        F.col("day_of_week").isin([1, 7])  # Sunday=1, Saturday=7
    )

logger.info("Daily metrics calculated")

# ── Step 5: Aggregate to facility level ───────────────────────
# Collapse all daily rows into one summary row per facility.
#
# pandas:
#   df.groupby("PROVNUM").agg({"col": "mean", "col2": "sum"})
#
# PySpark:
#   df.groupBy("PROVNUM").agg(F.mean("col"), F.sum("col2"))
#
# Key difference: PySpark groupBy is distributed across workers —
# each worker handles a subset of facilities in parallel.

logger.info("Aggregating to facility level...")

df_facility = df_daily.groupBy(
    "PROVNUM",
    "PROVNAME",
    "STATE",
    "CITY",
    "ownership_type",
    "provider_type",
    "certified_beds",
    "overall_rating",
    "staffing_rating",
    "nursing_turnover",
    "quarter"
).agg(

    # ── Patient volume ────────────────────────────────────────
    F.round(F.mean("MDScensus"), 1)
     .alias("avg_daily_census"),

    F.round(F.mean("bed_occupancy_rate"), 4)
     .alias("avg_bed_occupancy_rate"),

    # ── Staffing ratios ───────────────────────────────────────
    F.round(F.mean("CNA_hrs_per_patient"), 4)
     .alias("avg_CNA_hrs_per_patient"),

    F.round(F.mean("RN_hrs_per_patient"), 4)
     .alias("avg_RN_hrs_per_patient"),

    F.round(F.mean("LPN_hrs_per_patient"), 4)
     .alias("avg_LPN_hrs_per_patient"),

    F.round(F.mean("total_hrs_per_patient"), 4)
     .alias("avg_total_hrs_per_patient"),

    # ── Total hours worked ────────────────────────────────────
    F.round(F.sum("Hrs_RN"), 1)
     .alias("total_RN_hours"),

    F.round(F.sum("Hrs_LPN"), 1)
     .alias("total_LPN_hours"),

    F.round(F.sum("Hrs_CNA"), 1)
     .alias("total_CNA_hours"),

    # ── Contract vs employed breakdown ────────────────────────
    F.round(F.mean("contracted_rn_ratio"), 4)
     .alias("avg_contracted_rn_ratio"),

    F.round(F.sum("Hrs_RN_ctr"), 1)
     .alias("total_contracted_RN_hours"),

    F.round(F.sum("Hrs_RN_emp"), 1)
     .alias("total_employed_RN_hours"),

    # ── CMS compliance ────────────────────────────────────────
    # count days below CMS minimum — cast bool to int first
    # True=1, False=0 so sum() counts the True rows
    F.sum(F.col("meets_cms_minimums").cast("int"))
     .alias("days_meeting_cms_minimum"),

    F.count("WorkDate")
     .alias("total_days_in_quarter"),

    # ── Weekend vs weekday staffing ───────────────────────────
    F.round(
        F.mean(F.when(F.col("is_weekend"), F.col("total_hrs_per_patient"))),
        4
    ).alias("avg_weekend_hrs_per_patient"),

    F.round(
        F.mean(F.when(~F.col("is_weekend"), F.col("total_hrs_per_patient"))),
        4
    ).alias("avg_weekday_hrs_per_patient"),

)

# ── Step 6: Add derived facility-level columns ────────────────
# These can only be calculated AFTER aggregation because they
# depend on the aggregated values computed in Step 5.
logger.info("Adding derived facility-level columns...")

df_facility = df_facility \
    .withColumn(
        # % of days facility met ALL three CMS minimums
        "pct_days_meeting_cms",
        F.round(
            F.col("days_meeting_cms_minimum") /
            F.col("total_days_in_quarter") * 100,
            1
        )
    ) \
    .withColumn(
        # chronic understaffing flag —
        # facility below CMS minimum more than 50% of days
        "chronically_understaffed",
        F.col("pct_days_meeting_cms") < 50.0
    ) \
    .withColumn(
        # weekend staffing gap —
        # negative means worse staffing on weekends
        "weekend_staffing_gap",
        F.round(
            F.col("avg_weekend_hrs_per_patient") -
            F.col("avg_weekday_hrs_per_patient"),
            4
        )
    )

# quick sanity check
facility_count = df_facility.count()
logger.info(f"Facility summary rows: {facility_count:,}")

# show chronic understaffing breakdown
logger.info("Chronic understaffing summary:")
df_facility.groupBy("chronically_understaffed", "STATE") \
           .count() \
           .filter(F.col("chronically_understaffed") == True) \
           .orderBy("count", ascending=False) \
           .show(10)
           
# ── Step 7: Write Gold — facility summary Delta Lake table ────
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

# ── Step 8: Write Gold — staffing metrics Delta Lake table ────
logger.info("Writing Gold staffing metrics Delta Lake table...")

df_staffing_metrics = df_daily.select(
    "PROVNUM", "PROVNAME", "STATE", "WorkDate", "quarter",
    "MDScensus", "CNA_hrs_per_patient", "RN_hrs_per_patient",
    "LPN_hrs_per_patient", "total_hrs_per_patient",
    "bed_occupancy_rate", "contracted_rn_ratio",
    "meets_cms_minimums", "staffing_tier", "is_weekend",
    "day_of_week", "ownership_type", "overall_rating", "certified_beds",
)

delta_exists_staffing = DeltaTable.isDeltaTable(spark, GOLD_STAFFING_PATH)

if not delta_exists_staffing:
    logger.info("Gold staffing table does not exist — creating fresh...")
    df_staffing_metrics.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("STATE") \
        .save(GOLD_STAFFING_PATH)
    logger.info(f"Gold staffing table created at: {GOLD_STAFFING_PATH}")
else:
    logger.info("Gold staffing table exists — merging...")
    delta_table = DeltaTable.forPath(spark, GOLD_STAFFING_PATH)
    delta_table.alias("target").merge(
        df_staffing_metrics.alias("source"),
        "target.PROVNUM = source.PROVNUM AND "
        "target.WorkDate = source.WorkDate"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()
    logger.info("Gold staffing metrics merge completed")

# ── Step 9: Validate Gold tables ─────────────────────────────
logger.info("Validating Gold tables...")

df_val_facility = spark.read.format("delta").load(GOLD_FACILITY_PATH)
df_val_staffing = spark.read.format("delta").load(GOLD_STAFFING_PATH)

logger.info(f"Gold facility rows  : {df_val_facility.count():,}")
logger.info(f"Gold staffing rows  : {df_val_staffing.count():,}")

total     = df_val_facility.count()
compliant = df_val_facility \
    .filter(F.col("chronically_understaffed") == False).count()
pct = round(compliant / total * 100, 1)
logger.info(
    f"Facilities meeting CMS minimums >50% of days: "
    f"{compliant:,}/{total:,} ({pct}%)"
)

logger.info("Top 10 chronically understaffed facilities:")
df_val_facility \
    .filter(F.col("chronically_understaffed") == True) \
    .orderBy("pct_days_meeting_cms") \
    .select("PROVNAME", "STATE", "avg_CNA_hrs_per_patient",
            "pct_days_meeting_cms", "avg_daily_census") \
    .show(10, truncate=False)

# ── Step 10: Commit the Glue job ─────────────────────────────
logger.info("Silver to Gold job completed successfully")
job.commit()

