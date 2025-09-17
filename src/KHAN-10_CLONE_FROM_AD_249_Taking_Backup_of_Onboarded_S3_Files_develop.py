spark.catalog.setCurrentCatalog("purgo_databricks")

# PySpark script for Databricks: Automates migration of S3 files from landing to archive folder based on process log
# Purpose: Move files with file_status='SUCCESS' from S3 landing to archive folder using metadata from s3_file_process_log
# Author: Khanh Trinh
# Date: 2025-09-17
# Description: This script reads eligible file records from purgo_playground.s3_file_process_log, validates S3 paths and credentials, moves files from landing to archive using dbutils.fs.mv, and logs the operation in purgo_playground.s3_file_transfer_audit. It includes error handling, schema validation, and data quality checks.

# Import required modules for DataFrame operations and types
from pyspark.sql import functions as F  
from pyspark.sql.types import StringType, TimestampType  
from pyspark.sql import DataFrame  
import datetime  

# Load AWS credentials from Databricks secrets
try:
    access_key = dbutils.secrets.get(scope="aws_keys", key="access_key")  # Databricks secret
    secret_key = dbutils.secrets.get(scope="aws_keys", key="secret_key")  # Databricks secret
except Exception as e:
    # Log missing credentials error and exit
    raise RuntimeError("Missing AWS credentials in Databricks secret scope 'aws_keys'") from e

# Set AWS credentials for S3 access using Spark configuration
spark.conf.set("fs.s3a.access.key", access_key)
spark.conf.set("fs.s3a.secret.key", secret_key)

# Set current catalog and schema for Unity Catalog
spark.sql("USE CATALOG purgo_databricks")
spark.sql("USE purgo_playground")

def get_success_files_df() -> DataFrame:
    """
    Returns a DataFrame of files eligible for archiving (file_status='SUCCESS') from s3_file_process_log.
    Ensures schema consistency and correct data types.
    Returns:
        DataFrame: Filtered DataFrame with columns [file_name, s3_vendor_path, s3_landing_path, s3_archive_path, file_status, file_processed_date]
    """
    # Read the s3_file_process_log table
    try:
        df = spark.table("purgo_playground.s3_file_process_log")
    except Exception as e:
        raise RuntimeError("Error reading s3_file_process_log table") from e

    # Select and cast columns to enforce schema consistency
    df = df.select(
        F.col("file_name").cast(StringType()),
        F.col("s3_vendor_path").cast(StringType()),
        F.col("s3_landing_path").cast(StringType()),
        F.col("s3_archive_path").cast(StringType()),
        F.col("file_status").cast(StringType()),
        F.col("file_processed_date").cast(TimestampType())
    )

    # Filter for files with file_status='SUCCESS'
    df_success = df.filter(F.col("file_status") == "SUCCESS")

    # Data quality check: drop rows with null or empty file_name, s3_landing_path, s3_archive_path
    df_success = df_success.filter(
        (F.col("file_name").isNotNull()) & (F.length(F.col("file_name")) > 0) &
        (F.col("s3_landing_path").isNotNull()) & (F.length(F.col("s3_landing_path")) > 0) &
        (F.col("s3_archive_path").isNotNull()) & (F.length(F.col("s3_archive_path")) > 0)
    )

    return df_success

def validate_s3_path(s3_path: str) -> bool:
    """
    Validates that the S3 path is well-formed and starts with 's3://'.
    Args:
        s3_path (str): S3 path to validate
    Returns:
        bool: True if valid, False otherwise
    """
    return isinstance(s3_path, str) and s3_path.startswith("s3://") and len(s3_path) > 5

def move_file_s3(source_path: str, target_path: str) -> str:
    """
    Moves a file from source_path to target_path using dbutils.fs.mv.
    Args:
        source_path (str): Full S3 source file path
        target_path (str): Full S3 target file path
    Returns:
        str: Status message ("SUCCESS" or error message)
    """
    try:
        dbutils.fs.mv(source_path, target_path)
        return "SUCCESS"
    except Exception as e:
        return f"ERROR: {str(e)}"

def archive_files(df: DataFrame) -> list:
    """
    Iterates over eligible files and moves each from landing to archive folder.
    Logs status for each file.
    Args:
        df (DataFrame): DataFrame of eligible files
    Returns:
        list: List of dicts with file_name, source_path, target_path, status
    """
    results = []
    # Collect rows to driver for file operations (small batch assumed)
    rows = df.select("file_name", "s3_landing_path", "s3_archive_path").collect()
    for row in rows:
        file_name = row["file_name"]
        s3_landing_path = row["s3_landing_path"]
        s3_archive_path = row["s3_archive_path"]

        # Validate S3 paths
        if not validate_s3_path(s3_landing_path):
            status = "ERROR: Invalid landing path"
        elif not validate_s3_path(s3_archive_path):
            status = "ERROR: Invalid archive path"
        else:
            source_file = f"{s3_landing_path.rstrip('/')}/{file_name}"
            target_file = f"{s3_archive_path.rstrip('/')}/{file_name}"
            status = move_file_s3(source_file, target_file)
        results.append({
            "file_name": file_name,
            "source_path": s3_landing_path,
            "target_path": s3_archive_path,
            "status": status
        })
    return results

def log_file_transfer_audit(run_start_time: datetime.datetime, run_end_time: datetime.datetime) -> None:
    """
    Logs the file transfer audit to s3_file_transfer_audit table.
    Args:
        run_start_time (datetime.datetime): Start time of the run
        run_end_time (datetime.datetime): End time of the run
    Returns:
        None
    """
    # Create DataFrame for audit log
    audit_df = spark.createDataFrame(
        [(run_start_time, run_end_time)],
        ["run_start_time", "run_end_time"]
    )
    # Ensure schema consistency
    audit_df = audit_df.select(
        F.col("run_start_time").cast(TimestampType()),
        F.col("run_end_time").cast(TimestampType())
    )
    # Insert into audit table
    audit_df.write.format("delta").mode("append").saveAsTable("purgo_playground.s3_file_transfer_audit")

def log_operation_results(results: list) -> None:
    """
    Logs operation results for each file to driver log.
    Args:
        results (list): List of dicts with file_name, source_path, target_path, status
    Returns:
        None
    """
    for res in results:
        if res["status"] == "SUCCESS":
            print(f"File '{res['file_name']}' moved successfully from '{res['source_path']}' to '{res['target_path']}'")
        else:
            print(f"File '{res['file_name']}' not moved: {res['status']}")

# Main script execution
if __name__ == "__main__":
    # Record start time
    run_start_time = datetime.datetime.utcnow()

    # Get eligible files for archiving
    success_files_df = get_success_files_df()

    # Archive files and collect results
    operation_results = archive_files(success_files_df)

    # Log operation results
    log_operation_results(operation_results)

    # Record end time
    run_end_time = datetime.datetime.utcnow()

    # Log file transfer audit
    log_file_transfer_audit(run_start_time, run_end_time)

# End of script
