spark.catalog.setCurrentCatalog("purgo_databricks")

# Import necessary PySpark modules
from pyspark.sql import SparkSession  
from pyspark.sql.functions import col  
from pyspark.sql.types import StructType, StructField, StringType, TimestampType  

# Initialize Spark session
spark = SparkSession.builder \
    .appName("S3 File Migration Test") \
    .getOrCreate()

# Load AWS credentials from Databricks secrets
access_key = dbutils.secrets.get(scope="aws_keys", key="access_key")
secret_key = dbutils.secrets.get(scope="aws_keys", key="secret_key")

# Set AWS credentials for S3 access
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)

# Define the schema for the s3_file_process_log table
schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("s3_vendor_path", StringType(), True),
    StructField("s3_landing_path", StringType(), True),
    StructField("s3_archive_path", StringType(), True),
    StructField("file_status", StringType(), True),
    StructField("file_processed_date", TimestampType(), True)
])

# Read the s3_file_process_log table
try:
    s3_file_process_log_df = spark.read \
        .format("delta") \
        .schema(schema) \
        .table("purgo_playground.s3_file_process_log")
except Exception as e:
    print(f"Error reading s3_file_process_log table: {e}")

# Filter files with SUCCESS status
success_files_df = s3_file_process_log_df.filter(col("file_status") == "SUCCESS")

# Function to move files from landing to archive
def move_file(row):
    try:
        source_path = row.s3_landing_path
        target_path = row.s3_archive_path
        file_name = row.file_name

        # Construct full source and target paths
        full_source_path = f"{source_path}/{file_name}"
        full_target_path = f"{target_path}/{file_name}"

        # Move file from source to target
        dbutils.fs.mv(full_source_path, full_target_path)
        print(f"File {file_name} moved successfully from {source_path} to {target_path}")

    except Exception as e:
        print(f"Error moving file {row.file_name}: {e}")

# Apply the move_file function to each row in the DataFrame
success_files_df.rdd.foreach(move_file)

# Log the file transfer audit
try:
    audit_df = spark.createDataFrame([
        ("2024-03-21T00:00:00.000+0000", "2024-03-21T01:00:00.000+0000")
    ], ["run_start_time", "run_end_time"])

    audit_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("purgo_playground.s3_file_transfer_audit")
except Exception as e:
    print(f"Error logging file transfer audit: {e}")

# Stop the Spark session
spark.stop()
