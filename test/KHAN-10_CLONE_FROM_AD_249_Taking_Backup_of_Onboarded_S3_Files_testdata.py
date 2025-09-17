%pip install boto3
%pip install botocore

spark.catalog.setCurrentCatalog("purgo_databricks")

# PySpark script
# Purpose: Automate the migration of files from the Purgo S3 landing folder to the archive folder
# Author: Khanh Trinh
# Date: 2025-09-17
# Description: This script identifies files with a 'SUCCESS' status in the s3_file_process_log table and moves them from the landing folder to the archive folder in S3. It uses Databricks secrets for AWS credentials and logs the operations in the s3_file_transfer_audit table.

from pyspark.sql import SparkSession  
from pyspark.sql.functions import col, current_timestamp  
import boto3  
from botocore.exceptions import NoCredentialsError, PartialCredentialsError  

# Initialize Spark session
spark = SparkSession.builder.appName("S3 File Migration").getOrCreate()

def get_aws_credentials():
    """
    Retrieve AWS credentials from Databricks secrets.

    Returns:
        tuple: A tuple containing the access key and secret key.
    """
    try:
        access_key = dbutils.secrets.get(scope="aws_keys", key="access_key")
        secret_key = dbutils.secrets.get(scope="aws_keys", key="secret_key")
        return access_key, secret_key
    except Exception as e:
        print(f"Error retrieving AWS credentials: {e}")
        raise

def move_file_to_archive(s3_client, source_path, target_path):
    """
    Move a file from the source path to the target path in S3.

    Args:
        s3_client (boto3.client): The S3 client.
        source_path (str): The source S3 path.
        target_path (str): The target S3 path.

    Returns:
        bool: True if the file was moved successfully, False otherwise.
    """
    try:
        bucket_name, source_key = source_path.replace("s3://", "").split("/", 1)
        _, target_key = target_path.replace("s3://", "").split("/", 1)
        s3_client.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': source_key}, Key=target_key)
        s3_client.delete_object(Bucket=bucket_name, Key=source_key)
        return True
    except Exception as e:
        print(f"Error moving file from {source_path} to {target_path}: {e}")
        return False

def log_file_transfer_audit(run_start_time, run_end_time):
    """
    Log the file transfer operation in the s3_file_transfer_audit table.

    Args:
        run_start_time (timestamp): The start time of the operation.
        run_end_time (timestamp): The end time of the operation.
    """
    audit_data = [(run_start_time, run_end_time)]
    audit_df = spark.createDataFrame(audit_data, ["run_start_time", "run_end_time"])
    audit_df.write.insertInto("purgo_playground.s3_file_transfer_audit", overwrite=False)

def main():
    """
    Main function to execute the file migration process.
    """
    run_start_time = current_timestamp()

    # Retrieve AWS credentials
    access_key, secret_key = get_aws_credentials()

    # Initialize S3 client
    s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    # Read eligible files from the log table
    eligible_files_df = spark.sql("""
        SELECT file_name, s3_landing_path, s3_archive_path
        FROM purgo_playground.s3_file_process_log
        WHERE file_status = 'SUCCESS'
    """)

    # Process each eligible file
    for row in eligible_files_df.collect():
        source_path = row.s3_landing_path
        target_path = row.s3_archive_path
        if move_file_to_archive(s3_client, source_path, target_path):
            print(f"File {row.file_name} moved successfully from {source_path} to {target_path}")
        else:
            print(f"Failed to move file {row.file_name} from {source_path} to {target_path}")

    run_end_time = current_timestamp()
    log_file_transfer_audit(run_start_time, run_end_time)

if __name__ == "__main__":
    main()
