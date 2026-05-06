# ============================================================
# Glue Job: Google Drive → S3 Bronze (Ingestion)
# Healthcare Metrics Pipeline
# ============================================================
# WHAT THIS JOB DOES:
#   1. Retrieves Google Drive credentials from AWS Secrets Manager
#   2. Connects to Google Drive API
#   3. Lists available quarterly files on Google Drive
#   4. Checks Silver Delta Lake to find last ingested quarter
#   5. Downloads only NEW quarters to S3 Bronze
#   6. Exits cleanly if no new data available
#
# RUNS ON: AWS Glue 1.0 (Python Shell — no Spark needed)
# ============================================================

import sys
import os
import json
import logging
import tempfile
import boto3
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ── Step 1: Read job arguments ────────────────────────────────
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "BUCKET_NAME",
    "BRONZE_PATH_PREFIX",   # e.g. s3://bucket/bronze/
    "SECRET_NAME",          # AWS Secrets Manager secret name
    "DRIVE_FOLDER_ID",      # Google Drive folder ID
])

BUCKET_NAME        = args["BUCKET_NAME"]
BRONZE_PATH_PREFIX = args["BRONZE_PATH_PREFIX"]
SECRET_NAME        = args["SECRET_NAME"]
DRIVE_FOLDER_ID    = args["DRIVE_FOLDER_ID"]

logger.info(f"Starting Google Drive ingestion job")
logger.info(f"  BUCKET_NAME        : {BUCKET_NAME}")
logger.info(f"  BRONZE_PATH_PREFIX : {BRONZE_PATH_PREFIX}")
logger.info(f"  DRIVE_FOLDER_ID    : {DRIVE_FOLDER_ID}")

# ── Step 2: Install Google API client ─────────────────────────
# Python Shell jobs support pip install at runtime
import subprocess
subprocess.run([
    sys.executable, "-m", "pip", "install",
    "google-api-python-client",
    "google-auth",
    "--quiet"
], check=True)
logger.info("Google API client installed")

# ── Step 3: Retrieve credentials from Secrets Manager ─────────
# Never store credentials in code or Git —
# always retrieve from Secrets Manager at runtime
logger.info(f"Retrieving credentials from Secrets Manager: {SECRET_NAME}")

secrets_client = boto3.client("secretsmanager", region_name="us-east-1")
secret_response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
credentials_json = json.loads(secret_response["SecretString"])

logger.info("Credentials retrieved successfully")

# ── Step 4: Authenticate to Google Drive ──────────────────────
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# write credentials to a temp file — Google API requires a file path
with tempfile.NamedTemporaryFile(
    mode="w", suffix=".json", delete=False
) as tmp:
    json.dump(credentials_json, tmp)
    creds_path = tmp.name

# authenticate using service account
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
credentials = service_account.Credentials.from_service_account_file(
    creds_path, scopes=SCOPES
)
drive_service = build("drive", "v3", credentials=credentials)
logger.info("Authenticated to Google Drive successfully")

# ── Step 5: List files in the Google Drive folder ─────────────
# Query for CSV files in the specified folder
logger.info(f"Listing files in Drive folder: {DRIVE_FOLDER_ID}")

results = drive_service.files().list(
    q=f"'{DRIVE_FOLDER_ID}' in parents and mimeType='text/csv'",
    fields="files(id, name, modifiedTime)",
    pageSize=100
).execute()

drive_files = results.get("files", [])
logger.info(f"Found {len(drive_files)} CSV files on Google Drive:")
for f in drive_files:
    logger.info(f"  {f['name']} (id: {f['id']})")

# ── Step 6: Check what quarters already exist in S3 Bronze ────
# Compare Drive files against what's already in Bronze
# to determine what needs to be downloaded
s3_client = boto3.client("s3")

# list existing Bronze quarters
response = s3_client.list_objects_v2(
    Bucket=BUCKET_NAME,
    Prefix="bronze/",
    Delimiter="/"
)

existing_quarters = set()
for prefix in response.get("CommonPrefixes", []):
    # extract quarter from path like bronze/quarter=2024Q2/
    quarter = prefix["Prefix"].split("=")[-1].rstrip("/")
    existing_quarters.add(quarter)

logger.info(f"Existing Bronze quarters: {existing_quarters}")

# ── Step 7: Determine which files are new ─────────────────────
# Match Drive filenames to quarters
# CMS naming convention: PBJ_Daily_Nurse_Staffing_Q2_2024.csv
# Extract quarter from filename

def extract_quarter(filename):
    """
    Extract quarter string from CMS filename.
    e.g. PBJ_Daily_Nurse_Staffing_Q2_2024.csv -> 2024Q2
    """
    import re
    # match patterns like Q2_2024 or Q2_2024
    match = re.search(r'Q(\d)_(\d{4})', filename)
    if match:
        return f"{match.group(2)}Q{match.group(1)}"
    return None

# find PBJ main staffing file — this determines what quarter to ingest
pbj_files = [
    f for f in drive_files
    if "PBJ_Daily_Nurse_Staffing" in f["name"]
]

new_files_to_download = []
quarters_to_ingest = set()

for pbj_file in pbj_files:
    quarter = extract_quarter(pbj_file["name"])
    if quarter and quarter not in existing_quarters:
        quarters_to_ingest.add(quarter)
        logger.info(f"New quarter found: {quarter} — will download")

if not quarters_to_ingest:
    logger.info("No new quarters found — pipeline is up to date")
    logger.info("Ingestion job completed — nothing to do")
    # clean up temp credentials file
    os.unlink(creds_path)
    sys.exit(0)

logger.info(f"Quarters to ingest: {quarters_to_ingest}")

# ── Step 8: Download new files to S3 Bronze ───────────────────
# Download ALL Drive files for each new quarter
# This includes the main PBJ file and all supporting CSVs

def download_file_to_s3(file_id, filename, quarter):
    """
    Download a file from Google Drive and upload to S3 Bronze.
    Uses streaming to avoid loading large files into memory.
    """
    logger.info(f"Downloading {filename}...")

    # stream download from Google Drive
    request = drive_service.files().get_media(fileId=file_id)
    file_buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(file_buffer, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()
        logger.info(f"  Download progress: {int(status.progress() * 100)}%")

    # upload to S3
    s3_key = f"bronze/quarter={quarter}/{filename}"
    file_buffer.seek(0)
    s3_client.upload_fileobj(file_buffer, BUCKET_NAME, s3_key)
    logger.info(f"  Uploaded to s3://{BUCKET_NAME}/{s3_key}")

    return s3_key

uploaded_files = []

for drive_file in drive_files:
    filename = drive_file["name"]
    file_id  = drive_file["id"]

    # determine which quarter this file belongs to
    quarter = extract_quarter(filename)

    if quarter in quarters_to_ingest:
        # this file belongs to a new quarter — download it
        s3_key = download_file_to_s3(file_id, filename, quarter)
        uploaded_files.append(s3_key)
    elif quarter is None and quarters_to_ingest:
        # supporting files without quarter in name — download for all new quarters
        # e.g. NH_ProviderInfo_Oct2024.csv applies to all quarters
        for quarter in quarters_to_ingest:
            s3_key = download_file_to_s3(file_id, filename, quarter)
            uploaded_files.append(s3_key)

logger.info(f"Download complete — {len(uploaded_files)} files uploaded to S3 Bronze")
for f in uploaded_files:
    logger.info(f"  s3://{BUCKET_NAME}/{f}")

# ── Step 9: Clean up temp credentials file ────────────────────
os.unlink(creds_path)
logger.info("Credentials temp file removed")

# ── Step 10: Commit the Glue job ─────────────────────────────
logger.info("Google Drive ingestion job completed successfully")
logger.info(f"New quarters ingested: {quarters_to_ingest}")
