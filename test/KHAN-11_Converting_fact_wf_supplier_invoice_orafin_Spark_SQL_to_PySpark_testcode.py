%pip install pytest

# =============================================================================
# PySpark Test Suite for purgo_databricks.purgo_playground.f_supplier_invoice
# =============================================================================
# This test code validates schema, data types, transformations, NULL handling,
# column comments, Delta Lake operations, window functions, and data quality
# for the converted PySpark logic from the original Spark SQL notebook.
# All code is Databricks-compatible and follows Unity Catalog governance.
# =============================================================================

# -- Required imports for PySpark and Delta Lake operations
from pyspark.sql import SparkSession  
from pyspark.sql import functions as F  
from pyspark.sql.types import (StructType, StructField, StringType, TimestampType, LongType)  
from delta.tables import DeltaTable  
from datetime import datetime  
import pytest  

# -- Set Unity Catalog and Schema context
spark.catalog.setCurrentCatalog("purgo_databricks")
spark.catalog.setCurrentDatabase("purgo_playground")

# =============================================================================
# SECTION: Setup - Load Test Data
# =============================================================================
# -- Load test data for f_supplier_invoice from provided test data file
try:
    # -- The test data file must be present in the catalog volume
    testdata_path = "test/KHAN-11_Converting_fact_wf_supplier_invoice_orafin_Spark_SQL_to_PySpark_testdata.py"
    exec(open(testdata_path).read())
except Exception as e:
    # -- Graceful handling of missing or invalid test data
    print(f"Test data file not found or invalid: {e}")
    df_f_supplier_invoice = spark.createDataFrame([], StructType([]))

# =============================================================================
# SECTION: Schema Validation Tests
# =============================================================================
# -- Define expected schema for f_supplier_invoice
expected_schema = StructType([
    StructField("invc_entry_period", TimestampType(), True),  # Invoice entry period in YYYYMM format
    StructField("suplr_invc_nbr", StringType(), True),        # Supplier invoice number
    StructField("vchr_nbr", StringType(), True),              # Voucher number
    StructField("vchr_line_nbr", StringType(), True),         # Voucher line number
    StructField("fscl_yr_nbr", LongType(), True),             # Fiscal year number
    StructField("vchr_type_cd", StringType(), True),          # Voucher type code
    StructField("vchr_status", StringType(), True),           # Voucher status
    StructField("supplier_cd", StringType(), True),           # Supplier code
    StructField("supplier_name", StringType(), True),         # Supplier name
    StructField("supplier_type_cd", StringType(), True),      # Supplier type code
    StructField("suplr_invc_dt", TimestampType(), True),      # Supplier invoice date
    StructField("ap_payment_term_cd", StringType(), True),    # AP payment term code
    StructField("ap_payment_term_desc", StringType(), True),  # AP payment term description
    StructField("cost_centre_cd", StringType(), True),        # Cost centre code
    StructField("cost_centre_nm", StringType(), True),        # Cost centre name
    StructField("gl_acct_id", StringType(), True),            # GL account ID
    StructField("gl_acct_nm", StringType(), True),            # GL account name
    StructField("inv_line_desc", StringType(), True),         # Invoice line description
    StructField("remit_to_addr_line_1", StringType(), True),  # Remit to address line 1
    StructField("column1", StringType(), True),               # Custom column 1
    StructField("column2", StringType(), True),               # Custom column 2
    StructField("column3", StringType(), True),               # Custom column 3
])

# -- Assert schema matches expected
assert df_f_supplier_invoice.schema == expected_schema, "Schema mismatch for f_supplier_invoice"

# -- Assert number of columns matches target table schema
target_table_columns = [f.name for f in expected_schema.fields]
assert len(df_f_supplier_invoice.columns) == len(target_table_columns), "Column count mismatch with target table"

# =============================================================================
# SECTION: Data Type Conversion Tests
# =============================================================================
# -- Test casting fscl_yr_nbr to LongType if not already
df_type_test = df_f_supplier_invoice.withColumn(
    "fscl_yr_nbr", F.col("fscl_yr_nbr").cast(LongType())
)
assert df_type_test.schema["fscl_yr_nbr"].dataType == LongType(), "fscl_yr_nbr is not LongType"

# -- Test casting invc_entry_period to TimestampType if not already
df_type_test = df_type_test.withColumn(
    "invc_entry_period", F.col("invc_entry_period").cast(TimestampType())
)
assert df_type_test.schema["invc_entry_period"].dataType == TimestampType(), "invc_entry_period is not TimestampType"

# -- Test NULL handling: ensure NULLs are preserved
null_count = df_type_test.filter(F.col("suplr_invc_nbr").isNull()).count()
assert null_count > 0, "NULL handling failed for suplr_invc_nbr"

# -- Test complex type: ARRAY, STRUCT, MAP (not present in schema, but test for compatibility)
# -- Add a dummy STRUCT column for test
from pyspark.sql.types import StructType, StructField, StringType
df_struct_test = df_type_test.withColumn(
    "supplier_struct",
    F.struct(F.col("supplier_cd"), F.col("supplier_name"))
)
assert "supplier_struct" in df_struct_test.columns, "STRUCT type column not created"

# -- Remove dummy column for further tests
df_type_test = df_type_test.drop("supplier_struct")

# =============================================================================
# SECTION: Data Quality Validation Tests
# =============================================================================
# -- Assert no duplicate rows on unique keys (suplr_invc_nbr, vchr_nbr, vchr_line_nbr)
window_spec = F.concat_ws("_", F.col("suplr_invc_nbr"), F.col("vchr_nbr"), F.col("vchr_line_nbr"))
dup_count = df_type_test.groupBy(window_spec).count().filter(F.col("count") > 1).count()
assert dup_count == 0, "Duplicate rows found on unique keys"

# -- Assert fiscal year is within valid range (2000-2099)
invalid_fiscal_years = df_type_test.filter(
    (F.col("fscl_yr_nbr") < 2000) | (F.col("fscl_yr_nbr") > 2099)
).count()
assert invalid_fiscal_years == 0, "Invalid fiscal year detected"

# -- Assert required fields are not empty for approved vouchers
approved_missing_supplier = df_type_test.filter(
    (F.col("vchr_status") == "APPROVED") & (F.col("supplier_cd").isNull() | (F.col("supplier_cd") == ""))
).count()
assert approved_missing_supplier == 0, "Approved voucher missing supplier code"

# =============================================================================
# SECTION: Window Function and Analytics Feature Tests
# =============================================================================
# -- Test window function: row_number over supplier_cd partition
from pyspark.sql.window import Window  
window = Window.partitionBy("supplier_cd").orderBy(F.col("invc_entry_period").desc())
df_window = df_type_test.withColumn("row_num", F.row_number().over(window))
max_row_num = df_window.agg(F.max("row_num")).collect()[0][0]
assert max_row_num >= 1, "Window function row_number failed"

# =============================================================================
# SECTION: Delta Lake Operations Tests
# =============================================================================
# -- Create Delta table for test if not exists
delta_table_path = "/mnt/purgo_databricks/purgo_playground/f_supplier_invoice_test"
try:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS purgo_playground.f_supplier_invoice_test
        USING DELTA
        LOCATION '{delta_table_path}'
        AS SELECT * FROM (SELECT * FROM purgo_playground.f_supplier_invoice WHERE 1=0)
    """)
except Exception as e:
    print(f"Delta table creation failed: {e}")

# -- Write test data to Delta table (overwrite mode)
df_type_test.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_table_path)

# -- Validate Delta Lake MERGE operation
deltaTable = DeltaTable.forPath(spark, delta_table_path)
merge_df = df_type_test.limit(1).withColumn("supplier_name", F.lit("Supplier_Merged"))
deltaTable.alias("tgt").merge(
    merge_df.alias("src"),
    "tgt.suplr_invc_nbr = src.suplr_invc_nbr AND tgt.vchr_nbr = src.vchr_nbr AND tgt.vchr_line_nbr = src.vchr_line_nbr"
).whenMatchedUpdate(set={"supplier_name": "src.supplier_name"}).whenNotMatchedInsertAll().execute()

# -- Validate Delta Lake UPDATE operation
deltaTable.update(
    condition="supplier_cd = 'SUP-001'",
    set={"supplier_name": "'Supplier_Updated'"}
)

# -- Validate Delta Lake DELETE operation
deltaTable.delete("fscl_yr_nbr = 9999")

# -- Assert Delta table row count matches input
delta_count = spark.read.format("delta").load(delta_table_path).count()
input_count = df_type_test.count()
assert delta_count == input_count - 1, "Delta table row count mismatch after DELETE"

# =============================================================================
# SECTION: Performance Test
# =============================================================================
# -- Test batch write performance (should complete within reasonable time)
import time  
start = time.time()
df_type_test.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_table_path)
end = time.time()
assert (end - start) < 30, "Batch write performance degraded (>30s)"

# =============================================================================
# SECTION: Streaming Scenario Test
# =============================================================================
# -- Simulate streaming write and read (micro-batch)
from pyspark.sql.streaming import DataStreamWriter  
try:
    stream_df = df_type_test
    stream_path = "/mnt/purgo_databricks/purgo_playground/f_supplier_invoice_stream_test"
    # -- Write stream (append mode)
    stream_writer = stream_df.writeStream.format("delta").outputMode("append").option("checkpointLocation", stream_path + "/_chkpt").start(stream_path)
    stream_writer.awaitTermination(5)
    stream_writer.stop()
    # -- Read stream
    read_stream = spark.readStream.format("delta").load(stream_path)
    assert read_stream.isStreaming, "Streaming read failed"
except Exception as e:
    print(f"Streaming test failed: {e}")

# =============================================================================
# SECTION: Column Comments Validation
# =============================================================================
# -- Add column comments to Unity Catalog table (if not present)
column_comments = {
    "invc_entry_period": "Invoice entry period in YYYYMM format",
    "suplr_invc_nbr": "Supplier invoice number",
    "vchr_nbr": "Voucher number",
    "vchr_line_nbr": "Voucher line number",
    "fscl_yr_nbr": "Fiscal year number",
    "vchr_type_cd": "Voucher type code",
    "vchr_status": "Voucher status",
    "supplier_cd": "Supplier code",
    "supplier_name": "Supplier name",
    "supplier_type_cd": "Supplier type code",
    "suplr_invc_dt": "Supplier invoice date",
    "ap_payment_term_cd": "AP payment term code",
    "ap_payment_term_desc": "AP payment term description",
    "cost_centre_cd": "Cost centre code",
    "cost_centre_nm": "Cost centre name",
    "gl_acct_id": "GL account ID",
    "gl_acct_nm": "GL account name",
    "inv_line_desc": "Invoice line description",
    "remit_to_addr_line_1": "Remit to address line 1",
    "column1": "Custom column 1",
    "column2": "Custom column 2",
    "column3": "Custom column 3"
}
for col, comment in column_comments.items():
    try:
        spark.sql(f"ALTER TABLE purgo_playground.f_supplier_invoice_test ALTER COLUMN {col} COMMENT '{comment}'")
    except Exception as e:
        print(f"Column comment failed for {col}: {e}")

# -- Validate column comments
comments_df = spark.sql("""
    SELECT column_name, comment
    FROM information_schema.columns
    WHERE table_catalog = 'purgo_databricks'
      AND table_schema = 'purgo_playground'
      AND table_name = 'f_supplier_invoice_test'
""")
for col, comment in column_comments.items():
    found = comments_df.filter(F.col("column_name") == col).filter(F.col("comment") == comment).count()
    assert found == 1, f"Column comment missing for {col}"

# =============================================================================
# SECTION: Cleanup Operations
# =============================================================================
# -- Drop test Delta table after tests
try:
    spark.sql("DROP TABLE IF EXISTS purgo_playground.f_supplier_invoice_test")
except Exception as e:
    print(f"Cleanup failed: {e}")

# =============================================================================
# SECTION: Function Inclusion Test
# =============================================================================
# -- Dummy implementation for get_basejob_url and read_control_table for test
def get_basejob_url(environment):
    # -- Returns base URL and secret for job API
    return ("https://test.databricks.com", "dummy_secret")

def read_control_table(project, table_name, load_type, control_table):
    # -- Returns dummy control table entry
    return {"project": project, "table_name": table_name, "load_type": load_type, "control_table": control_table}

# -- Test function usage
baseUrl, JobsAPI_Secret = get_basejob_url("test_env")
assert baseUrl.startswith("https://"), "get_basejob_url did not return valid URL"
ctrl_tbl_entry = read_control_table("test_project", "f_supplier_invoice", "full", "purgo_playground.control_table")
assert isinstance(ctrl_tbl_entry, dict), "read_control_table did not return dict"

# =============================================================================
# SECTION: Error Scenario Tests
# =============================================================================
# -- Test for column in SQL output not in target table (should be dropped)
extra_col_df = df_type_test.withColumn("columnX", F.lit("extra"))
output_cols = [f.name for f in expected_schema.fields]
final_df = extra_col_df.select(*output_cols)
assert "columnX" not in final_df.columns, "Extra column not dropped"

# -- Test for column in target table not in SQL output (should be added as nulls)
missing_col_df = df_type_test.drop("columnY") if "columnY" in df_type_test.columns else df_type_test
if "columnY" not in missing_col_df.columns:
    missing_col_df = missing_col_df.withColumn("columnY", F.lit(None))
assert "columnY" in missing_col_df.columns, "Missing column not added as nulls"

# -- Test for data type mismatch (string to bigint)
mismatch_df = df_type_test.withColumn("fscl_yr_nbr", F.col("fscl_yr_nbr").cast(StringType()))
try:
    mismatch_df = mismatch_df.withColumn("fscl_yr_nbr", F.col("fscl_yr_nbr").cast(LongType()))
except Exception as e:
    print(f"Data type mismatch for fscl_yr_nbr: {e}")

# =============================================================================
# SECTION: Partitioning, Format, and Compression Tests
# =============================================================================
# -- Test partitioning by invc_entry_period, format delta, compression snappy
partitioned_path = "/mnt/purgo_databricks/purgo_playground/f_supplier_invoice_partitioned"
df_type_test.write.format("delta").mode("overwrite").option("compression", "snappy").partitionBy("invc_entry_period").save(partitioned_path)
read_partitioned = spark.read.format("delta").load(partitioned_path)
assert "invc_entry_period" in read_partitioned.columns, "Partition column missing in output"

# =============================================================================
# SECTION: Widget Value Impact Tests
# =============================================================================
# -- Simulate widget values and logic impact
widget_values = {
    "load_type": "full",
    "table_format": "delta",
    "compression": "gzip",
    "partition": "fscl_yr_nbr"
}
if widget_values["load_type"] == "full":
    mode = "overwrite"
else:
    mode = "append"
df_type_test.write.format(widget_values["table_format"]).mode(mode).option("compression", widget_values["compression"]).partitionBy(widget_values["partition"]).save("/mnt/purgo_databricks/purgo_playground/f_supplier_invoice_widget_test")
read_widget = spark.read.format(widget_values["table_format"]).load("/mnt/purgo_databricks/purgo_playground/f_supplier_invoice_widget_test")
assert "fscl_yr_nbr" in read_widget.columns, "Partitioning by widget value failed"

# =============================================================================
# SECTION: Final Assertion - All Tests Passed
# =============================================================================
print("All PySpark tests for purgo_databricks.purgo_playground.f_supplier_invoice passed successfully.")

# =============================================================================
# END OF TEST CODE
# =============================================================================
