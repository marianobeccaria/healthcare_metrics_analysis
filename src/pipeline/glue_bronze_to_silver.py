# ============================================================
# Glue Job: Bronze → Silver
# Healthcare Metrics Pipeline
# ============================================================
# WHAT THIS JOB DOES:
#   1. Reads raw CSVs from S3 Bronze
#   2. Applies all EDA-informed cleaning rules
#   3. Joins PBJ staffing data with NH_ProviderInfo
#   4. Routes unmatched CCNs to audit table
#   5. Writes cleaned data as Delta Lake to S3 Silver
#
# RUNS ON: AWS Glue 4.0 (Spark 3.3, Python 3.10)
# ============================================================

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, FloatType, IntegerType, DateType
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from delta.tables import DeltaTable

# ── Logging setup 
# In Glue, print() works but logging is more professional
# These logs stream to CloudWatch in real time
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ── Step 1: Read job arguments 
# getResolvedOptions reads the --ARGUMENT_NAME values
# defined in the CDK stack default_arguments
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "BUCKET_NAME",
    "BRONZE_PATH",
    "SILVER_PATH",
    "AUDIT_PATH",
    "QUARTER",
])

BUCKET_NAME = args["BUCKET_NAME"]
BRONZE_PATH = args["BRONZE_PATH"]
SILVER_PATH = args["SILVER_PATH"]
AUDIT_PATH  = args["AUDIT_PATH"]
QUARTER     = args["QUARTER"]

logger.info(f"Starting Bronze → Silver job")
logger.info(f"  BRONZE_PATH : {BRONZE_PATH}")
logger.info(f"  SILVER_PATH : {SILVER_PATH}")
logger.info(f"  QUARTER     : {QUARTER}")

# ── Step 2: Initialize Spark and Glue context 
# GlueContext wraps SparkContext
# SparkSession is the main entry point to all Spark operations
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

# ── Step 3: Read PBJ staffing CSV from Bronze 
# In PySpark, reading a file is a TRANSFORMATION — nothing
# actually loads until we trigger an action later.
#
# Key differences from pandas read_csv:
#   - No nrows= (PySpark handles large files natively)
#   - No encoding= on the reader (set via option())
#   - schema inference is slow — we enforce types manually after

logger.info("Reading PBJ staffing data from Bronze...")

df_pbj_raw = spark.read \
    .option("header", "true") \
    .option("encoding", "latin-1") \
    .option("inferSchema", "false") \
    .csv(BRONZE_PATH + "PBJ_Daily_Nurse_Staffing_Q2_2024.csv")

# Log how many rows we loaded — .count() is an ACTION
# this is the first time Spark actually reads the file
row_count = df_pbj_raw.count()
logger.info(f"PBJ raw rows loaded: {row_count:,}")

# ── Step 4: Read NH_ProviderInfo CSV from Bronze 
logger.info("Reading NH_ProviderInfo from Bronze...")

df_provider_raw = spark.read \
    .option("header", "true") \
    .option("encoding", "latin-1") \
    .option("inferSchema", "false") \
    .csv(BRONZE_PATH + "NH_ProviderInfo_Oct2024.csv")

logger.info(f"ProviderInfo raw rows loaded: {df_provider_raw.count():,}")

# ── Step 5: Peek at the schema 
# printSchema() shows column names and inferred types
# equivalent to df.dtypes in pandas
# This logs to CloudWatch for debugging
logger.info("PBJ schema:")
df_pbj_raw.printSchema()


# ── Step 6: Enforce correct data types on PBJ data 
# With inferSchema=false everything loaded as StringType.
# We now cast each column to its correct type explicitly.
#
# In pandas you'd do: df["col"] = df["col"].astype(float)
# In PySpark you use: df.withColumn("col", F.col("col").cast(Type))
#
# withColumn() either:
#   - replaces an existing column (same name)
#   - adds a new column (new name)
logger.info("Enforcing data types on PBJ data...")

# hours columns — all need to be float
hours_cols = [
    "Hrs_RNDON", "Hrs_RNDON_emp", "Hrs_RNDON_ctr",
    "Hrs_RNadmin", "Hrs_RNadmin_emp", "Hrs_RNadmin_ctr",
    "Hrs_RN", "Hrs_RN_emp", "Hrs_RN_ctr",
    "Hrs_LPNadmin", "Hrs_LPNadmin_emp", "Hrs_LPNadmin_ctr",
    "Hrs_LPN", "Hrs_LPN_emp", "Hrs_LPN_ctr",
    "Hrs_CNA", "Hrs_CNA_emp", "Hrs_CNA_ctr",
    "Hrs_NAtrn", "Hrs_NAtrn_emp", "Hrs_NAtrn_ctr",
    "Hrs_MedAide", "Hrs_MedAide_emp", "Hrs_MedAide_ctr",
]

# start from raw, same principle as df_raw.copy() in pandas
# never mutate the raw DataFrame
df_pbj = df_pbj_raw

# cast all hours columns to float in a loop
# equivalent to pandas: df[col] = df[col].astype(float)
for col in hours_cols:
    df_pbj = df_pbj.withColumn(col, F.col(col).cast(FloatType()))

# cast MDScensus to integer, it's a patient count
df_pbj = df_pbj.withColumn("MDScensus", F.col("MDScensus").cast(IntegerType()))

# parse WorkDate as a proper date type
# F.to_date() is the PySpark equivalent of pd.to_datetime()
df_pbj = df_pbj.withColumn(
    "WorkDate",
    F.to_date(F.col("WorkDate"), "yyyyMMdd")
)

# PROVNUM and COUNTY_FIPS stay as StringType — already loaded as string
# This is the leading zeros protection from our EDA findings

logger.info("Data types enforced successfully")

# ── Step 7: Filter low census rows 
# Exploratory Data Analysis EDA finding:
#   MDScensus < 10 are reopening/edge case facilities
#   These skew ratio calculations — exclude before any metrics
#
# Equivalent to pandas: df = df[df["MDScensus"] >= 10]
# In PySpark .filter() and .where() are identical — use either
logger.info("Filtering low census rows (MDScensus < 10)...")

df_pbj_filtered = df_pbj.filter(F.col("MDScensus") >= 10)

# log how many rows were excluded
excluded = row_count - df_pbj_filtered.count()
logger.info(f"Rows excluded (low census): {excluded:,}")

# ── Step 8: Enforce types on ProviderInfo 
# We only need a subset of columns from ProviderInfo
# Selecting only what we need reduces memory and speeds up the join.
# Equivalent to pandas: df[["col1", "col2", "col3"]]
logger.info("Selecting and typing ProviderInfo columns...")

df_provider = df_provider_raw.select(
    # join key — must stay as string (leading zeros)
    F.col("CMS Certification Number (CCN)").alias("CCN"),

    # facility context columns
    F.col("Ownership Type").alias("ownership_type"),
    F.col("Provider Type").alias("provider_type"),

    # numeric columns for metrics
    F.col("Number of Certified Beds") \
        .cast(FloatType()).alias("certified_beds"),
    F.col("Overall Rating") \
        .cast(FloatType()).alias("overall_rating"),
    F.col("Staffing Rating") \
        .cast(FloatType()).alias("staffing_rating"),
    F.col("Total nursing staff turnover") \
        .cast(FloatType()).alias("nursing_turnover"),
    F.col("Reported RN Staffing Hours per Resident per Day") \
        .cast(FloatType()).alias("reported_rn_hrs"),
    F.col("Reported CNA Staffing Hours per Resident per Day") \
        .cast(FloatType()).alias("reported_cna_hrs"),
)

logger.info(f"ProviderInfo columns selected: {len(df_provider.columns)}")

# ── Step 9: JOIN PBJ to ProviderInfo 
# Exploratory Data Analysis finding: 
#   99.9% match rate, LEFT JOIN confirmed
#   LEFT JOIN keeps ALL PBJ rows Unmatched get null ProviderInfo cols
#
# PySpark join syntax:
#   df1.join(df2, condition, how)
#
# Pandas equivalent:
#   df_pbj.merge(df_provider, left_on="PROVNUM",
#                right_on="CCN", how="left")
logger.info("Joining PBJ to ProviderInfo...")

df_joined = df_pbj_filtered.join(
    df_provider,
    df_pbj_filtered["PROVNUM"] == df_provider["CCN"],
    how="left"
)

# ── Step 10: Separate matched vs unmatched rows 
# Exploratory Data Analysis finding: 
#   17 facilities had no CCN match in ProviderInfo
#   Route them to audit table
#
# F.col("CCN").isNull() is equivalent to pandas:
#   df[df["CCN"].isna()]
df_matched   = df_joined.filter(F.col("CCN").isNotNull())
df_unmatched = df_joined.filter(F.col("CCN").isNull())

matched_count   = df_matched.count()
unmatched_count = df_unmatched.count()

logger.info(f"Matched rows   : {matched_count:,}")
logger.info(f"Unmatched rows : {unmatched_count:,}")

# ── Step 11: Add derived columns 
# These are the core metrics from Step 2:
# hours per patient per day for each nurse type
#
# F.when() is PySpark's equivalent of np.where() or if/else
# Avoid division by zero with when() condition.

logger.info("Adding derived ratio columns...")

df_silver = df_matched \
    .withColumn(
        "CNA_hrs_per_patient",
        F.when(F.col("MDScensus") > 0,
               F.col("Hrs_CNA") / F.col("MDScensus")
        ).otherwise(F.lit(None))
    ) \
    .withColumn(
        "RN_hrs_per_patient",
        F.when(F.col("MDScensus") > 0,
               F.col("Hrs_RN") / F.col("MDScensus")
        ).otherwise(F.lit(None))
    ) \
    .withColumn(
        "total_hrs_per_patient",
        F.when(F.col("MDScensus") > 0,
               (F.col("Hrs_RN") + F.col("Hrs_LPN") + F.col("Hrs_CNA"))
               / F.col("MDScensus")
        ).otherwise(F.lit(None))
    ) \
    .withColumn(
        # contracted ratio: what % of RN hours are contracted?
        "contracted_rn_ratio",
        F.when(F.col("Hrs_RN") > 0,
               F.col("Hrs_RN_ctr") / F.col("Hrs_RN")
        ).otherwise(F.lit(0.0))
    ) \
    .withColumn(
        # CMS minimum staffing tier from EDA findings
        # same logic as the staffing_tier() function in Step 2
        "staffing_tier",
        F.when(F.col("CNA_hrs_per_patient").isNull(),
               "exclude_zero_census")
        .when(F.col("CNA_hrs_per_patient") == 0,
              "critical_no_staff")
        .when(F.col("CNA_hrs_per_patient") < 1,
              "critical_understaffed")
        .when(F.col("CNA_hrs_per_patient") < 2.45,
              "below_cms_minimum")
        .otherwise("meets_cms_minimum")
    ) \
    .withColumn(
        # add quarter as a partition column
        "quarter", F.lit(QUARTER)
    )

logger.info("Derived columns added successfully")

# Quick sanity check. Show tier distribution
logger.info("Staffing tier distribution:")
df_silver.groupBy("staffing_tier") \
         .count() \
         .orderBy("count", ascending=False) \
         .show()
         
# ── Step 12: Write unmatched rows to audit table 
# Handle the unmatched rows FIRST before writing silver
# to ensure no data is silently dropped.
#
# Write as plain Parquet (not Delta) for the audit table since
# it's a simple log, not a queryable table that needs ACID.
#
# PySpark write syntax:
#   df.write.format().mode().option().save(path)
#
# mode="append" - add to existing data (don't overwrite)
# mode="overwrite" - replace everything
# mode="ignore" - skip if data already exists
# mode="error" - fail if data already exists (default)

logger.info(f"Writing {unmatched_count:,} unmatched rows to audit table...")

if unmatched_count > 0:
    df_unmatched.write \
        .format("parquet") \
        .mode("append") \
        .save(AUDIT_PATH)
    logger.info(f"Audit table written to: {AUDIT_PATH}")
else:
    logger.info("No unmatched rows — audit table not written")


# ── Step 13: Write Silver Delta Lake table 
# Delta Lake adds a _delta_log/ folder alongside the Parquet files
# that tracks every write as a transaction.
#
# First run: creates the Delta table from scratch
# Subsequent runs: MERGE - inserts only new rows, skips existing
#
# Why MERGE instead of overwrite?
#   overwrite - rewrites ALL 1.3M rows every quarter (slow, risky)
#   MERGE     - only inserts new quarter rows.

logger.info("Writing Silver Delta Lake table...")

# check if Delta table already exists
# if it does MERGE, if not create fresh
delta_table_exists = DeltaTable.isDeltaTable(spark, SILVER_PATH)

if not delta_table_exists:
    # ── First run: create the Delta table 
    logger.info("Delta table does not exist — creating fresh...")

    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("STATE") \
        .save(SILVER_PATH)

    logger.info(f"Delta table created at: {SILVER_PATH}")

else:
    # ── Subsequent runs: MERGE new quarter data 
    # MERGE is Delta Lake's upsert operation:
    #   - match on PROVNUM + WorkDate (unique key per row)
    #   - if match found -> skip (row already exists)
    #   - if no match -> insert new row
    #
    # This means running the job twice on the same quarter
    # is completely safe — no duplicates created
    logger.info("Delta table exists — merging new quarter data...")

    delta_table = DeltaTable.forPath(spark, SILVER_PATH)

    delta_table.alias("target").merge(
        df_silver.alias("source"),
        # match condition — one row per facility per day
        "target.PROVNUM = source.PROVNUM AND "
        "target.WorkDate = source.WorkDate"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()

    logger.info("Merge completed successfully")

# ── Step 14: Validate the Silver table 
# After writing, always read back and validate.
# This catches write failures that don't raise exceptions.
logger.info("Validating Silver table...")

df_validate = spark.read.format("delta").load(SILVER_PATH)
silver_count = df_validate.count()

logger.info(f"Silver table total rows : {silver_count:,}")
logger.info(f"Silver table columns    : {len(df_validate.columns)}")

# check staffing tier distribution in the full silver table
logger.info("Silver table staffing tier distribution:")
df_validate.groupBy("staffing_tier", "quarter") \
           .count() \
           .orderBy("quarter", "staffing_tier") \
           .show()

# ── Step 15: Commit the Glue job 
# job.commit() tells Glue the job finished successfully.
# Without this the Glue console shows the job as still running
# even after the script exits.
logger.info("Bronze → Silver job completed successfully")
job.commit()

