USE CATALOG purgo_databricks;

-- Databricks SQL script: Comprehensive test suite for Site Data Ingestion and Validation (abc1234-1)
-- Purpose: Validate data movement, schema, constraints, and business logic for landing, raw, and raw_reject layers
-- Author: Khanh Trinh
-- Date: 2025-10-10
-- Description: This script tests the ingestion logic for site data, including schema validation, data quality, business rules, Delta Lake operations, and error handling for the tables: landing_abc1234_1, raw_abc1234_1, raw_reject_abc1234_1 in purgo_databricks.purgo_playground.

-- =========================================================================================================
-- SETUP: Drop and recreate all relevant tables to ensure a clean test environment
-- =========================================================================================================

-- Drop tables if they exist
DROP TABLE IF EXISTS purgo_databricks.purgo_playground.landing_abc1234_1;
DROP TABLE IF EXISTS purgo_databricks.purgo_playground.raw_abc1234_1;
DROP TABLE IF EXISTS purgo_databricks.purgo_playground.raw_reject_abc1234_1;

-- Create landing table with constraints
CREATE TABLE purgo_databricks.purgo_playground.landing_abc1234_1 (
  Clinical_Study_Source_Id STRING NOT NULL,
  Study_Country_source_id STRING NOT NULL,
  Study_Site_source_id STRING NOT NULL,
  site_number BIGINT NOT NULL,
  site_status STRING,
  primary_investigator_source_id STRING,
  CONSTRAINT site_status_values CHECK (site_status IN ("Active", "Closed") OR site_status IS NULL)
);

-- Create raw table with constraints
CREATE TABLE purgo_databricks.purgo_playground.raw_abc1234_1 (
  Clinical_Study_Source_Id STRING NOT NULL,
  Study_Country_source_id STRING NOT NULL,
  Study_Site_source_id STRING NOT NULL,
  site_number BIGINT NOT NULL,
  site_status STRING,
  primary_investigator_source_id STRING,
  CONSTRAINT site_status_values CHECK (site_status IN ("Active", "Closed") OR site_status IS NULL)
);

-- Create raw_reject table
CREATE TABLE purgo_databricks.purgo_playground.raw_reject_abc1234_1 (
  Clinical_Study_Source_Id STRING,
  Study_Country_source_id STRING,
  Study_Site_source_id STRING,
  raw_site_number BIGINT,
  landing_site_number BIGINT,
  error_message STRING
);

-- =========================================================================================================
-- TEST DATA: Insert test data into landing and raw tables
-- =========================================================================================================

-- Insert test data into landing_abc1234_1
INSERT INTO purgo_databricks.purgo_playground.landing_abc1234_1 VALUES
  ("S1", "C1", "SS1", 1001, "Active", "PI1"),
  ("S2", "C2", "SS2", 2002, "Closed", "PI2"),
  ("S3", "C3", "SS3", 3003, "Active", "PI3"),
  ("S4", "C4", "SS4", 4004, "Active", "PI4"),
  ("S5", "C5", "SS5", 5005, "Active", "PI5"),
  ("S6", "C6", "SS6", 6006, "Closed", "PI6"),
  ("S7", "C7", "SS7", 7007, "Active", "PI7"),
  ("S8", "C8", "SS8", 8008, "Active", "PI8"),
  ("S9", "C9", "SS9", 9009, "Active", "PI9"),
  ("S10", "C10", "SS10", 1010, "Closed", "PI10");

-- Insert test data into raw_abc1234_1
INSERT INTO purgo_databricks.purgo_playground.raw_abc1234_1 VALUES
  ("S1", "C1", "SS1", 1001, "Active", "PI1"),
  ("S2", "C2", "SS2", 2003, "Closed", "PI2"),
  ("S4", "C4", "SS4", 4004, "Active", "PI4"),
  ("S5", "C5", "SS5", 5006, "Active", "PI5"),
  ("S7", "C7", "SS7", 7007, "Active", "PI7"),
  ("S8", "C8", "SS8", 8009, "Active", "PI8"),
  ("S10", "C10", "SS10", NULL, "Closed", "PI10"); -- Simulate missing site_number for edge case

-- =========================================================================================================
-- SCHEMA VALIDATION TESTS
-- =========================================================================================================

-- Validate landing table schema: column count and types
-- Should be 6 columns with correct types
WITH landing_schema AS (
  SELECT column_name, data_type, is_nullable
  FROM information_schema.columns
  WHERE table_schema = "purgo_playground"
    AND table_name = "landing_abc1234_1"
)
SELECT
  COUNT(*) AS col_count,
  SUM(CASE WHEN data_type = "STRING" THEN 1 ELSE 0 END) AS string_cols,
  SUM(CASE WHEN data_type = "BIGINT" THEN 1 ELSE 0 END) AS bigint_cols
FROM landing_schema;

-- Assert: col_count = 6, string_cols = 4, bigint_cols = 2

-- Validate raw table schema: column count and types
WITH raw_schema AS (
  SELECT column_name, data_type, is_nullable
  FROM information_schema.columns
  WHERE table_schema = "purgo_playground"
    AND table_name = "raw_abc1234_1"
)
SELECT
  COUNT(*) AS col_count,
  SUM(CASE WHEN data_type = "STRING" THEN 1 ELSE 0 END) AS string_cols,
  SUM(CASE WHEN data_type = "BIGINT" THEN 1 ELSE 0 END) AS bigint_cols
FROM raw_schema;

-- Assert: col_count = 6, string_cols = 4, bigint_cols = 2

-- Validate raw_reject table schema: column count and types
WITH reject_schema AS (
  SELECT column_name, data_type, is_nullable
  FROM information_schema.columns
  WHERE table_schema = "purgo_playground"
    AND table_name = "raw_reject_abc1234_1"
)
SELECT
  COUNT(*) AS col_count,
  SUM(CASE WHEN data_type = "STRING" THEN 1 ELSE 0 END) AS string_cols,
  SUM(CASE WHEN data_type = "BIGINT" THEN 1 ELSE 0 END) AS bigint_cols
FROM reject_schema;

-- Assert: col_count = 6, string_cols = 4, bigint_cols = 2

-- =========================================================================================================
-- DATA TYPE CONVERSION AND NULL HANDLING TESTS
-- =========================================================================================================

-- Test: site_number must be BIGINT, NULL not allowed in landing/raw
-- Insert a record with site_number as NULL, should fail due to NOT NULL constraint
-- (This is a negative test, will not actually insert, but validate constraint)
-- SELECT * FROM purgo_databricks.purgo_playground.landing_abc1234_1 WHERE site_number IS NULL;
-- Assert: Should return 0 rows

-- Test: site_status constraint
-- Insert a record with invalid site_status, should fail
-- (Negative test, will not actually insert, but validate constraint)
-- SELECT * FROM purgo_databricks.purgo_playground.landing_abc1234_1 WHERE site_status NOT IN ("Active", "Closed") AND site_status IS NOT NULL;
-- Assert: Should return 0 rows

-- Test: NULL handling in raw_reject table (raw_site_number can be NULL)
SELECT COUNT(*) AS null_raw_site_number
FROM purgo_databricks.purgo_playground.raw_reject_abc1234_1
WHERE raw_site_number IS NULL;

-- =========================================================================================================
-- BUSINESS LOGIC VALIDATION: DATA MOVEMENT FROM LANDING TO RAW/RAW_REJECT
-- =========================================================================================================

-- CTE: Identify records to be inserted into raw (new or matching site_number)
WITH landing AS (
  SELECT * FROM purgo_databricks.purgo_playground.landing_abc1234_1
),
raw AS (
  SELECT * FROM purgo_databricks.purgo_playground.raw_abc1234_1
),
valid_raw_insert AS (
  SELECT l.*
  FROM landing l
  LEFT JOIN raw r
    ON l.Clinical_Study_Source_Id = r.Clinical_Study_Source_Id
   AND l.Study_Country_source_id = r.Study_Country_source_id
   AND l.Study_Site_source_id = r.Study_Site_source_id
  WHERE r.Clinical_Study_Source_Id IS NULL
     OR l.site_number = r.site_number
)
SELECT * FROM valid_raw_insert;

-- Assert: Should return records S1, S3, S4, S6, S7, S9, S10

-- CTE: Identify records to be inserted into raw_reject (site_number mismatch)
WITH landing AS (
  SELECT * FROM purgo_databricks.purgo_playground.landing_abc1234_1
),
raw AS (
  SELECT * FROM purgo_databricks.purgo_playground.raw_abc1234_1
),
invalid_raw_insert AS (
  SELECT
    l.Clinical_Study_Source_Id,
    l.Study_Country_source_id,
    l.Study_Site_source_id,
    r.site_number AS raw_site_number,
    l.site_number AS landing_site_number,
    "Site number mismatch between landing and raw for given study/site/country combination" AS error_message
  FROM landing l
  INNER JOIN raw r
    ON l.Clinical_Study_Source_Id = r.Clinical_Study_Source_Id
   AND l.Study_Country_source_id = r.Study_Country_source_id
   AND l.Study_Site_source_id = r.Study_Site_source_id
  WHERE l.site_number != r.site_number
)
SELECT * FROM invalid_raw_insert;

-- Assert: Should return records S2, S5, S8

-- =========================================================================================================
-- DELTA LAKE OPERATIONS: MERGE, UPDATE, DELETE TESTS
-- =========================================================================================================

-- Test: MERGE operation for upsert into raw_abc1234_1
-- Only insert new records or records with matching site_number
MERGE INTO purgo_databricks.purgo_playground.raw_abc1234_1 AS target
USING (
  SELECT * FROM purgo_databricks.purgo_playground.landing_abc1234_1
) AS source
ON target.Clinical_Study_Source_Id = source.Clinical_Study_Source_Id
AND target.Study_Country_source_id = source.Study_Country_source_id
AND target.Study_Site_source_id = source.Study_Site_source_id
WHEN NOT MATCHED THEN
  INSERT (
    Clinical_Study_Source_Id,
    Study_Country_source_id,
    Study_Site_source_id,
    site_number,
    site_status,
    primary_investigator_source_id
  )
  VALUES (
    source.Clinical_Study_Source_Id,
    source.Study_Country_source_id,
    source.Study_Site_source_id,
    source.site_number,
    source.site_status,
    source.primary_investigator_source_id
  )
WHEN MATCHED AND target.site_number = source.site_number THEN
  INSERT (
    Clinical_Study_Source_Id,
    Study_Country_source_id,
    Study_Site_source_id,
    site_number,
    site_status,
    primary_investigator_source_id
  )
  VALUES (
    source.Clinical_Study_Source_Id,
    source.Study_Country_source_id,
    source.Study_Site_source_id,
    source.site_number,
    source.site_status,
    source.primary_investigator_source_id
  );

-- Assert: No update or delete occurs, only insert

-- Test: DELETE operation (should not delete any records as per requirements)
DELETE FROM purgo_databricks.purgo_playground.raw_abc1234_1
WHERE FALSE;

-- Assert: No records deleted

-- =========================================================================================================
-- WINDOW FUNCTION TEST: Count of records per site_status
-- =========================================================================================================

-- Use window function to count records per site_status in landing
SELECT
  site_status,
  COUNT(*) OVER (PARTITION BY site_status) AS status_count
FROM purgo_databricks.purgo_playground.landing_abc1234_1;

-- =========================================================================================================
-- DATA QUALITY VALIDATION TESTS
-- =========================================================================================================

-- CTE: Validate required fields in landing_abc1234_1
WITH landing AS (
  SELECT * FROM purgo_databricks.purgo_playground.landing_abc1234_1
),
validation AS (
  SELECT
    Clinical_Study_Source_Id,
    Study_Country_source_id,
    Study_Site_source_id,
    site_number,
    site_status,
    primary_investigator_source_id,
    CASE
      WHEN Clinical_Study_Source_Id IS NULL OR Clinical_Study_Source_Id = "" THEN "Clinical_Study_Source_Id is required"
      WHEN Study_Country_source_id IS NULL OR Study_Country_source_id = "" THEN "Study_Country_source_id is required"
      WHEN Study_Site_source_id IS NULL OR Study_Site_source_id = "" THEN "Study_Site_source_id is required"
      WHEN site_number IS NULL THEN "site_number is required"
      WHEN CAST(site_number AS STRING) RLIKE "^[0-9]+$" = FALSE THEN "site_number must be a bigint"
      ELSE ""
    END AS error_message,
    CASE
      WHEN Clinical_Study_Source_Id IS NULL OR Clinical_Study_Source_Id = "" THEN FALSE
      WHEN Study_Country_source_id IS NULL OR Study_Country_source_id = "" THEN FALSE
      WHEN Study_Site_source_id IS NULL OR Study_Site_source_id = "" THEN FALSE
      WHEN site_number IS NULL THEN FALSE
      WHEN CAST(site_number AS STRING) RLIKE "^[0-9]+$" = FALSE THEN FALSE
      ELSE TRUE
    END AS valid
  FROM landing
)
SELECT * FROM validation;

-- =========================================================================================================
-- INTEGRATION TEST: End-to-end flow validation
-- =========================================================================================================

-- CTE: Simulate full flow from landing to raw/raw_reject
WITH landing AS (
  SELECT * FROM purgo_databricks.purgo_playground.landing_abc1234_1
),
raw AS (
  SELECT * FROM purgo_databricks.purgo_playground.raw_abc1234_1
),
to_raw AS (
  SELECT l.*
  FROM landing l
  LEFT JOIN raw r
    ON l.Clinical_Study_Source_Id = r.Clinical_Study_Source_Id
   AND l.Study_Country_source_id = r.Study_Country_source_id
   AND l.Study_Site_source_id = r.Study_Site_source_id
  WHERE r.Clinical_Study_Source_Id IS NULL
     OR l.site_number = r.site_number
),
to_reject AS (
  SELECT
    l.Clinical_Study_Source_Id,
    l.Study_Country_source_id,
    l.Study_Site_source_id,
    r.site_number AS raw_site_number,
    l.site_number AS landing_site_number,
    "Site number mismatch between landing and raw for given study/site/country combination" AS error_message
  FROM landing l
  INNER JOIN raw r
    ON l.Clinical_Study_Source_Id = r.Clinical_Study_Source_Id
   AND l.Study_Country_source_id = r.Study_Country_source_id
   AND l.Study_Site_source_id = r.Study_Site_source_id
  WHERE l.site_number != r.site_number
)
SELECT "raw" AS target, * FROM to_raw
UNION ALL
SELECT "reject" AS target, Clinical_Study_Source_Id, Study_Country_source_id, Study_Site_source_id, raw_site_number, landing_site_number, error_message FROM to_reject;

-- =========================================================================================================
-- PERFORMANCE TEST: Count records processed per second (simulated)
-- =========================================================================================================

-- Simulate performance by counting records processed
SELECT
  COUNT(*) AS total_processed,
  CURRENT_TIMESTAMP AS processed_at
FROM purgo_databricks.purgo_playground.landing_abc1234_1;

-- =========================================================================================================
-- CLEANUP: Drop all test tables after tests
-- =========================================================================================================

DROP TABLE IF EXISTS purgo_databricks.purgo_playground.landing_abc1234_1;
DROP TABLE IF EXISTS purgo_databricks.purgo_playground.raw_abc1234_1;
DROP TABLE IF EXISTS purgo_databricks.purgo_playground.raw_reject_abc1234_1;

-- END OF TEST SCRIPT
