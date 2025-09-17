# Databricks PySpark script to automate migration of files from Purgo S3 landing folder to archive folder
# References s3_file_process_log table to identify eligible files for archiving (file_status = 'SUCCESS')
# Dynamically reads s3_landing_path and s3_archive_path for each file from the log table
# Uses Databricks secrets for AWS credentials (scope: aws_keys, keys: access_key, secret_key)
# Logs file transfer audit in purgo_playground.s3_file_transfer_audit
# Handles error cases and logs appropriate messages
# Ensures schema consistency and proper NULL handling
# All code uses DataFrame APIs and Databricks best practices

# Set current catalog to Unity Catalog
spark.catalog.setCurrentCatalog("purgo_databricks")  # built-in

# Import required PySpark modules
from pyspark.sql.functions import col, current_timestamp  
from pyspark.sql.types import StringType, TimestampType, StructType, StructField  

# Load AWS credentials from Databricks secrets
try:
    access_key = dbutils.secrets.get(scope="aws_keys", key="access_key")  # databricks
    secret_key = dbutils.secrets.get(scope="aws_keys", key="secret_key")  # databricks
except Exception as cred_ex:
    # Log missing credentials error
    print("Missing AWS credentials in Databricks secret scope 'aws_keys'.")
    raise cred_ex

# Set AWS credentials for S3 access using Hadoop configuration
spark.conf.set("fs.s3a.access.key", access_key)  # databricks
spark.conf.set("fs.s3a.secret.key", secret_key)  # databricks

# Define schema for s3_file_process_log table for validation
s3_file_process_log_schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("s3_vendor_path", StringType(), True),
    StructField("s3_landing_path", StringType(), True),
    StructField("s3_archive_path", StringType(), True),
    StructField("file_status", StringType(), True),
    StructField("file_processed_date", TimestampType(), True)
])  # pyspark

# Read s3_file_process_log table and validate schema
try:
    s3_file_process_log_df = spark.read.table("purgo_playground.s3_file_process_log")  # databricks
    # Enforce column order and types to match schema
    s3_file_process_log_df = s3_file_process_log_df.select(
        col("file_name").cast(StringType()),
        col("s3_vendor_path").cast(StringType()),
        col("s3_landing_path").cast(StringType()),
        col("s3_archive_path").cast(StringType()),
        col("file_status").cast(StringType()),
        col("file_processed_date").cast(TimestampType())
    )
except Exception as read_ex:
    print(f"Error reading s3_file_process_log table: {read_ex}")
    raise read_ex

# CTE: Select eligible files for archiving (file_status = 'SUCCESS')
# Only files with valid non-null paths and file_name are considered
eligible_files_cte = (
    s3_file_process_log_df
    .filter(
        (col("file_status") == "SUCCESS") &
        (col("file_name").isNotNull()) &
        (col("s3_landing_path").isNotNull()) &
        (col("s3_archive_path").isNotNull())
    )
    .select(
        col("file_name"),
        col("s3_landing_path"),
        col("s3_archive_path")
    )
)

# Collect eligible files to driver for file operations
eligible_files = eligible_files_cte.collect()  # databricks

# Initialize audit log variables
run_start_time = None
run_end_time = None

# Start audit log timer
from datetime import datetime  
run_start_time = datetime.utcnow()

# Track file move results for logging
file_move_results = []

# Move each eligible file from landing to archive folder
for file_row in eligible_files:
    file_name = file_row["file_name"]
    s3_landing_path = file_row["s3_landing_path"]
    s3_archive_path = file_row["s3_archive_path"]

    # Validate S3 paths format
    if not (s3_landing_path.startswith("s3://") and s3_archive_path.startswith("s3://")):
        file_move_results.append({
            "file_name": file_name,
            "result": "FAILED",
            "message": "Invalid S3 path format"
        })
        continue

    # Construct full source and target file paths
    source_file_path = f"{s3_landing_path.rstrip('/')}/{file_name}"
    target_file_path = f"{s3_archive_path.rstrip('/')}/{file_name}"

    try:
        # Move file from landing to archive using dbutils.fs.mv
        dbutils.fs.mv(source_file_path, target_file_path)  # databricks
        file_move_results.append({
            "file_name": file_name,
            "result": "SUCCESS",
            "message": f"File moved successfully from {source_file_path} to {target_file_path}"
        })
    except Exception as mv_ex:
        file_move_results.append({
            "file_name": file_name,
            "result": "FAILED",
            "message": f"Error moving file: {str(mv_ex)}"
        })

# End audit log timer
run_end_time = datetime.utcnow()

# Log file transfer audit in s3_file_transfer_audit table
try:
    # Prepare audit DataFrame with schema validation
    audit_schema = StructType([
        StructField("run_start_time", TimestampType(), True),
        StructField("run_end_time", TimestampType(), True)
    ])  # pyspark

    audit_data = [(run_start_time, run_end_time)]
    audit_df = spark.createDataFrame(audit_data, schema=audit_schema)  # databricks

    # Ensure column order and types match target table
    audit_df = audit_df.select(
        col("run_start_time").cast(TimestampType()),
        col("run_end_time").cast(TimestampType())
    )

    # Insert audit log into s3_file_transfer_audit table
    audit_df.write.format("delta").mode("append").saveAsTable("purgo_playground.s3_file_transfer_audit")  # databricks
except Exception as audit_ex:
    print(f"Error logging file transfer audit: {audit_ex}")

# Log file move results for monitoring and troubleshooting
for result in file_move_results:
    print(f"File: {result['file_name']}, Result: {result['result']}, Message: {result['message']}")

# End of script
# All file operations and audit logging completed
