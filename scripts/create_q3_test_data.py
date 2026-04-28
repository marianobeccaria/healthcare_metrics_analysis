# Adds a new Quarter to do an end-to-end test of the workflow

import pandas as pd
import numpy as np

# load a small sample of Q2 data
df = pd.read_csv(
    "data/raw/PBJ_Daily_Nurse_Staffing_Q2_2024.csv",
    nrows=5000,
    dtype={"PROVNUM": str},
    encoding="latin-1"
)

print(f"Loaded {len(df):,} rows from Q2")
print(f"Date range: {df['WorkDate'].min()} to {df['WorkDate'].max()}")

# shift dates from Q2 (Apr-Jun) to Q3 (Jul-Sep)
# Q2 dates are in format YYYYMMDD
df["WorkDate"] = df["WorkDate"].astype(str)
df["WorkDate"] = df["WorkDate"] \
    .str.replace("20240401", "20240701") \
    .str.replace("20240402", "20240702") \
    .str.replace("20240403", "20240703") \
    .str.replace("20240404", "20240704") \
    .str.replace("20240405", "20240705") \
    .str.replace("20240406", "20240706") \
    .str.replace("20240407", "20240707") \
    .str.replace("20240408", "20240708") \
    .str.replace("20240409", "20240709") \
    .str.replace("20240410", "20240710") \
    .str.replace("20240411", "20240711") \
    .str.replace("20240412", "20240712") \
    .str.replace("20240413", "20240713") \
    .str.replace("20240414", "20240714") \
    .str.replace("20240415", "20240715") \
    .str.replace("20240416", "20240716") \
    .str.replace("20240417", "20240717") \
    .str.replace("20240418", "20240718") \
    .str.replace("20240419", "20240719") \
    .str.replace("20240420", "20240720") \
    .str.replace("20240421", "20240721") \
    .str.replace("20240422", "20240722") \
    .str.replace("20240423", "20240723") \
    .str.replace("20240424", "20240724") \
    .str.replace("20240425", "20240725") \
    .str.replace("20240426", "20240726") \
    .str.replace("20240427", "20240727") \
    .str.replace("20240428", "20240728") \
    .str.replace("20240429", "20240729") \
    .str.replace("20240430", "20240730") \
    .str.replace("20240501", "20240801") \
    .str.replace("20240502", "20240802") \
    .str.replace("20240503", "20240803") \
    .str.replace("20240504", "20240804") \
    .str.replace("20240505", "20240805") \
    .str.replace("20240506", "20240806") \
    .str.replace("20240507", "20240807") \
    .str.replace("20240508", "20240808") \
    .str.replace("20240509", "20240809") \
    .str.replace("20240510", "20240810") \
    .str.replace("20240601", "20240901") \
    .str.replace("20240602", "20240902") \
    .str.replace("20240603", "20240903") \
    .str.replace("20240604", "20240904") \
    .str.replace("20240605", "20240905") \
    .str.replace("20240606", "20240906") \
    .str.replace("20240607", "20240907") \
    .str.replace("20240608", "20240908") \
    .str.replace("20240609", "20240909") \
    .str.replace("20240610", "20240910")

# update the quarter column
df["CY_Qtr"] = "2024Q3"

# save as Q3 test file
output_path = "data/raw/PBJ_Daily_Nurse_Staffing_Q3_2024.csv"
df.to_csv(output_path, index=False, encoding="latin-1")

print(f"Q3 test file created: {output_path}")
print(f"Rows: {len(df):,}")
print(f"Date range: {df['WorkDate'].min()} to {df['WorkDate'].max()}")
print(f"Quarter: {df['CY_Qtr'].iloc[0]}")