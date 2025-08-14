/* 
    Databricks SQL Test Suite for patient_account Table Creation and Data Validation
    Catalog: purgo_databricks
    Schema: purgo_playground
    Table: patient_account
    Author: Automated Test Code
*/

/* ---------------------------------------------------------------------------
   SECTION: Setup - Drop and Create Table
--------------------------------------------------------------------------- */

USE CATALOG purgo_databricks;

-- Drop patient_account table if it already exists
DROP TABLE IF EXISTS purgo_databricks.purgo_playground.patient_account;

-- Create patient_account table with required schema, constraints, and comments
CREATE TABLE purgo_databricks.purgo_playground.patient_account (
    patient_foundation_shipment STRING NOT NULL COMMENT 'Patient foundation shipment identifier',
    prescriber_id STRING NOT NULL COMMENT 'Unique prescriber identifier',
    prescriber_key STRING NOT NULL COMMENT 'Prescriber key for internal mapping',
    patient_sf_id STRING NOT NULL COMMENT 'Salesforce patient ID',
    service_request_type STRING NOT NULL COMMENT 'Type of service request',
    case_sf_id STRING NOT NULL COMMENT 'Salesforce case ID',
    account_id STRING NOT NULL COMMENT 'Account identifier',
    hash_key STRING NOT NULL COMMENT 'Hash key for record uniqueness',
    last_modified_date TIMESTAMP NOT NULL COMMENT 'Last modified timestamp',
    planned_date DATE COMMENT 'Planned date for patient account',
    CONSTRAINT service_request_type_values CHECK (service_request_type IN ("NEW", "UPDATE", "CLOSE")),
    CONSTRAINT prescriber_id_format CHECK (prescriber_id RLIKE "^[A-Za-z0-9_-]{6,32}$"),
    CONSTRAINT patient_sf_id_format CHECK (patient_sf_id RLIKE "^[A-Za-z0-9]{8,18}$"),
    CONSTRAINT case_sf_id_format CHECK (case_sf_id RLIKE "^[A-Za-z0-9]{8,18}$"),
    CONSTRAINT account_id_format CHECK (account_id RLIKE "^[A-Za-z0-9]{8,18}$")
)
COMMENT 'Centralized patient account information for healthcare management';

-- Add unique constraint for prescriber_key per prescriber_id
ALTER TABLE purgo_databricks.purgo_playground.patient_account
ADD CONSTRAINT unique_prescriber_key_per_prescriber_id UNIQUE (prescriber_id, prescriber_key);

/* ---------------------------------------------------------------------------
   SECTION: Unit Test - Schema Validation
--------------------------------------------------------------------------- */

-- Validate that the table schema matches the specification
SELECT 
    column_name,
    data_type,
    is_nullable,
    comment
FROM purgo_databricks.information_schema.columns
WHERE table_catalog = "purgo_databricks"
  AND table_schema = "purgo_playground"
  AND table_name = "patient_account"
ORDER BY ordinal_position;

-- Assert: The number of columns must be 10
SELECT 
    COUNT(*) AS column_count
FROM purgo_databricks.information_schema.columns
WHERE table_catalog = "purgo_databricks"
  AND table_schema = "purgo_playground"
  AND table_name = "patient_account";
-- Expected: column_count = 10

/* ---------------------------------------------------------------------------
   SECTION: Unit Test - Column Comments
--------------------------------------------------------------------------- */

-- Validate that all column comments are present and correct
SELECT 
    column_name,
    comment
FROM purgo_databricks.information_schema.columns
WHERE table_catalog = "purgo_databricks"
  AND table_schema = "purgo_playground"
  AND table_name = "patient_account"
ORDER BY ordinal_position;

/* ---------------------------------------------------------------------------
   SECTION: Integration Test - Insert Valid Data (Happy Path)
--------------------------------------------------------------------------- */

-- Insert valid rows
INSERT INTO purgo_databricks.purgo_playground.patient_account (
    patient_foundation_shipment, prescriber_id, prescriber_key, patient_sf_id, service_request_type, case_sf_id, account_id, hash_key, last_modified_date, planned_date
) VALUES
    ("SHIP123456", "PRSC7890", "KEY001", "PAT12345678", "NEW", "CASE876543", "ACC1234567", sha2(concat("SHIP123456", "|", "PRSC7890", "|", "PAT12345678", "|", "ACC1234567"), 256), CAST("2024-06-01T12:34:56.000+0000" AS TIMESTAMP), CAST("2024-06-15" AS DATE)),
    ("SHIP654321", "PRSC1234", "KEY002", "PAT87654321", "UPDATE", "CASE123456", "ACC7654321", sha2(concat("SHIP654321", "|", "PRSC1234", "|", "PAT87654321", "|", "ACC7654321"), 256), CAST("2024-06-02T08:00:00.000+0000" AS TIMESTAMP), NULL);

-- Validate inserted rows
SELECT *
FROM purgo_databricks.purgo_playground.patient_account
WHERE patient_foundation_shipment IN ("SHIP123456", "SHIP654321");

/* ---------------------------------------------------------------------------
   SECTION: Integration Test - Insert Invalid Data (Error Scenarios)
--------------------------------------------------------------------------- */

-- Attempt to insert invalid prescriber_id (should fail due to constraint)
INSERT INTO purgo_databricks.purgo_playground.patient_account (
    patient_foundation_shipment, prescriber_id, prescriber_key, patient_sf_id, service_request_type, case_sf_id, account_id, hash_key, last_modified_date, planned_date
) VALUES
    ("SHIPERR01", "PRSC!@#", "KEY007", "PATERR01", "NEW", "CASEERR01", "ACCERR01", sha2(concat("SHIPERR01", "|", "PRSC!@#", "|", "PATERR01", "|", "ACCERR01"), 256), CAST("2024-06-07T12:00:00.000+0000" AS TIMESTAMP), CAST("2024-06-23" AS DATE));
-- Expected error: "Invalid prescriber_id format"

-- Attempt to insert invalid service_request_type (should fail due to constraint)
INSERT INTO purgo_databricks.purgo_playground.patient_account (
    patient_foundation_shipment, prescriber_id, prescriber_key, patient_sf_id, service_request_type, case_sf_id, account_id, hash_key, last_modified_date, planned_date
) VALUES
    ("SHIPERR03", "PRSCERR03", "KEY009", "PATERR03", "INVALID", "CASEERR03", "ACCERR03", sha2(concat("SHIPERR03", "|", "PRSCERR03", "|", "PATERR03", "|", "ACCERR03"), 256), CAST("2024-06-09T14:00:00.000+0000" AS TIMESTAMP), CAST("2024-06-24" AS DATE));
-- Expected error: "Invalid service_request_type value"

-- Attempt to insert NULL required field (should fail due to NOT NULL constraint)
INSERT INTO purgo_databricks.purgo_playground.patient_account (
    patient_foundation_shipment, prescriber_id, prescriber_key, patient_sf_id, service_request_type, case_sf_id, account_id, hash_key, last_modified_date, planned_date
) VALUES
    (NULL, "PRSCERR07", "KEY013", "PATERR07", "NEW", "CASEERR07", "ACCERR07", sha2(concat("", "|", "PRSCERR07", "|", "PATERR07", "|", "ACCERR07"), 256), CAST("2024-06-12T17:00:00.000+0000" AS TIMESTAMP), CAST("2024-06-26" AS DATE));
-- Expected error: "Field patient_foundation_shipment is required and cannot be null/empty"

/* ---------------------------------------------------------------------------
   SECTION: Data Quality Validation - Hash Key Generation
--------------------------------------------------------------------------- */

-- Validate hash_key generation logic
WITH hash_test AS (
    SELECT
        "SHIP123456" AS patient_foundation_shipment,
        "PRSC7890" AS prescriber_id,
        "PAT12345678" AS patient_sf_id,
        "ACC1234567" AS account_id,
        sha2(concat("SHIP123456", "|", "PRSC7890", "|", "PAT12345678", "|", "ACC1234567"), 256) AS expected_hash
)
SELECT
    pa.hash_key = ht.expected_hash AS hash_key_valid
FROM purgo_databricks.purgo_playground.patient_account pa
JOIN hash_test ht
  ON pa.patient_foundation_shipment = ht.patient_foundation_shipment
 AND pa.prescriber_id = ht.prescriber_id
 AND pa.patient_sf_id = ht.patient_sf_id
 AND pa.account_id = ht.account_id;
-- Expected: hash_key_valid = true

/* ---------------------------------------------------------------------------
   SECTION: Data Quality Validation - Planned Date Optionality
--------------------------------------------------------------------------- */

-- Validate planned_date is stored as NULL when not provided
SELECT
    planned_date IS NULL AS planned_date_is_null
FROM purgo_databricks.purgo_playground.patient_account
WHERE patient_foundation_shipment = "SHIP654321";
-- Expected: planned_date_is_null = true

-- Validate planned_date is stored correctly when provided
SELECT
    planned_date = CAST("2024-06-15" AS DATE) AS planned_date_valid
FROM purgo_databricks.purgo_playground.patient_account
WHERE patient_foundation_shipment = "SHIP123456";
-- Expected: planned_date_valid = true

/* ---------------------------------------------------------------------------
   SECTION: Uniqueness Constraint Test - prescriber_key per prescriber_id
--------------------------------------------------------------------------- */

-- Attempt to insert duplicate prescriber_key for prescriber_id (should fail)
INSERT INTO purgo_databricks.purgo_playground.patient_account (
    patient_foundation_shipment, prescriber_id, prescriber_key, patient_sf_id, service_request_type, case_sf_id, account_id, hash_key, last_modified_date, planned_date
) VALUES
    ("SHIPDUPLICATE", "PRSC7890", "KEY001", "PATDUPLICATE", "NEW", "CASEDUPLICATE", "ACCDUPLICATE", sha2(concat("SHIPDUPLICATE", "|", "PRSC7890", "|", "PATDUPLICATE", "|", "ACCDUPLICATE"), 256), CAST("2024-07-02T00:00:00.000+0000" AS TIMESTAMP), CAST("2024-07-02" AS DATE));
-- Expected error: "Duplicate prescriber_key for prescriber_id"

/* ---------------------------------------------------------------------------
   SECTION: Window Function Test - Analytics Feature
--------------------------------------------------------------------------- */

-- Test window function: count of accounts per prescriber_id
SELECT
    prescriber_id,
    COUNT(account_id) OVER (PARTITION BY prescriber_id) AS account_count_per_prescriber
FROM purgo_databricks.purgo_playground.patient_account
ORDER BY prescriber_id;

/* ---------------------------------------------------------------------------
   SECTION: Delta Lake Operations Test
--------------------------------------------------------------------------- */

-- Test MERGE operation: update planned_date for a specific account
MERGE INTO purgo_databricks.purgo_playground.patient_account AS target
USING (
    SELECT "SHIP123456" AS patient_foundation_shipment, CAST("2024-07-15" AS DATE) AS new_planned_date
) AS source
ON target.patient_foundation_shipment = source.patient_foundation_shipment
WHEN MATCHED THEN
    UPDATE SET planned_date = source.new_planned_date;

-- Validate planned_date update
SELECT
    planned_date = CAST("2024-07-15" AS DATE) AS planned_date_updated
FROM purgo_databricks.purgo_playground.patient_account
WHERE patient_foundation_shipment = "SHIP123456";
-- Expected: planned_date_updated = true

-- Test DELETE operation: remove a test row
DELETE FROM purgo_databricks.purgo_playground.patient_account
WHERE patient_foundation_shipment = "SHIP654321";

-- Validate row deletion
SELECT
    COUNT(*) AS deleted_row_count
FROM purgo_databricks.purgo_playground.patient_account
WHERE patient_foundation_shipment = "SHIP654321";
-- Expected: deleted_row_count = 0

/* ---------------------------------------------------------------------------
   SECTION: Cleanup
--------------------------------------------------------------------------- */

-- Clean up test rows (optional, for idempotency)
DELETE FROM purgo_databricks.purgo_playground.patient_account
WHERE patient_foundation_shipment LIKE "SHIP%";

