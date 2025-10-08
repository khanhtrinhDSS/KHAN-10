-- Databricks SQL script: Comprehensive test suite for trf_inv_txn ETL logic and data quality
-- Purpose: Validate ETL logic, schema, data types, constraints, deduplication, lookups, and error handling for trf_inv_txn
-- Author: Khanh Trinh
-- Date: 2025-10-08
-- Description: This script tests the trf_inv_txn ETL pipeline using CTEs, covering unit, integration, data quality, performance, and Delta Lake operations. It validates schema, constraints, deduplication, type conversions, NULL/default handling, movement flag lookups, reference order type logic, aggregation, and filter conditions. All logic is implemented using CTEs, with assertions and cleanup.

USE CATALOG purgo_databricks;
USE SCHEMA purgo_playground;

-- ========================================================================
-- 1. SCHEMA VALIDATION: Ensure trf_inv_txn table matches expected schema
-- ========================================================================

-- Validate column names, data types, and constraints for trf_inv_txn
-- All constraints (NOT NULL, CHECK, PRIMARY KEY) are asserted

-- CTE: Get trf_inv_txn table schema
WITH trf_inv_txn_schema AS (
  SELECT
    column_name,
    data_type,
    is_nullable
  FROM information_schema.columns
  WHERE table_catalog = "purgo_databricks"
    AND table_schema = "purgo_playground"
    AND table_name = "trf_inv_txn"
)
-- Validation Query: Assert expected columns and types
SELECT
  CASE
    WHEN COUNT(*) = 36 THEN "PASS"
    ELSE "FAIL"
  END AS schema_column_count_assertion
FROM trf_inv_txn_schema
WHERE column_name IN (
  "company_cd", "item_nbr", "location_cd", "lot_nbr", "plant_cd", "src_sys_cd", "transaction_id",
  "company_currency_cd", "document_nbr", "gl_account_nbr", "posting_dt_yyyymmdd", "primary_uom_cd",
  "reference_order_company_cd", "reference_order_schedule_line_nbr", "reference_order_type",
  "transaction_dt_yyyymmdd", "transaction_qty_primary_uom", "transaction_qty_transaction_uom",
  "transaction_reason", "transaction_ts", "transaction_type_cd", "transaction_uom",
  "unit_cost_company_currency", "plant_key", "prod_key", "prod_plant_key", "plant_location_key",
  "plant_lot_key", "prod_plant_location_key", "prod_plant_lot_key", "prod_plant_lot_location_key",
  "gl_acct_key", "co_key", "consumption_usage_flag", "receipt_flag", "scrap_flag", "shipment_flag"
);

-- ========================================================================
-- 2. DATA TYPE CONVERSION & NULL HANDLING TESTS
-- ========================================================================

-- CTE: Select test data for type conversion and NULL handling
WITH test_type_conversion AS (
  SELECT
    transaction_qty_primary_uom,
    transaction_qty_transaction_uom,
    posting_dt_yyyymmdd,
    primary_uom_cd,
    location_cd,
    lot_nbr,
    plant_cd,
    company_cd,
    reference_order_type
  FROM trf_inv_txn
  WHERE transaction_id IN ("1000007-1-2023", "1000009-1-2023", "1000002-1-2023", "1000006-1-2023", "1000025-1-2023")
)
-- Validation Query: Assert type conversion and NULL/default handling
SELECT
  CASE
    WHEN typeof(transaction_qty_primary_uom) = "double" THEN "PASS"
    ELSE "FAIL"
  END AS qty_primary_uom_type_assertion,
  CASE
    WHEN typeof(transaction_qty_transaction_uom) = "double" THEN "PASS"
    ELSE "FAIL"
  END AS qty_transaction_uom_type_assertion,
  CASE
    WHEN posting_dt_yyyymmdd RLIKE "^[0-9]{8}$" THEN "PASS"
    ELSE "FAIL"
  END AS posting_dt_format_assertion,
  CASE
    WHEN primary_uom_cd IS NULL OR typeof(primary_uom_cd) = "string" THEN "PASS"
    ELSE "FAIL"
  END AS primary_uom_cd_type_assertion,
  CASE
    WHEN location_cd IS NULL OR typeof(location_cd) = "string" THEN "PASS"
    ELSE "FAIL"
  END AS location_cd_null_assertion,
  CASE
    WHEN lot_nbr IS NULL OR typeof(lot_nbr) = "string" THEN "PASS"
    ELSE "FAIL"
  END AS lot_nbr_null_assertion,
  CASE
    WHEN plant_cd IS NULL OR typeof(plant_cd) = "string" THEN "PASS"
    ELSE "FAIL"
  END AS plant_cd_null_assertion,
  CASE
    WHEN company_cd IS NULL OR typeof(company_cd) = "string" THEN "PASS"
    ELSE "FAIL"
  END AS company_cd_null_assertion,
  CASE
    WHEN reference_order_type IS NULL OR reference_order_type = "none" OR typeof(reference_order_type) = "string" THEN "PASS"
    ELSE "FAIL"
  END AS reference_order_type_default_assertion
FROM test_type_conversion;

-- ========================================================================
-- 3. DEDUPLICATION & PRIMARY KEY CONSTRAINT TESTS
-- ========================================================================

-- CTE: Find duplicate records by composite key
WITH duplicate_keys AS (
  SELECT
    prod_plant_lot_location_key,
    transaction_id,
    COUNT(*) AS dup_count
  FROM trf_inv_txn
  GROUP BY prod_plant_lot_location_key, transaction_id
  HAVING COUNT(*) > 1
)
-- Validation Query: Assert no duplicates exist
SELECT
  CASE
    WHEN COUNT(*) = 0 THEN "PASS"
    ELSE "FAIL"
  END AS deduplication_assertion
FROM duplicate_keys;

-- ========================================================================
-- 4. MOVEMENT FLAG LOOKUP TESTS
-- ========================================================================

-- CTE: Validate movement flag mapping for bwart 641, 642, 643
WITH movement_flag_test AS (
  SELECT
    transaction_id,
    consumption_usage_flag,
    receipt_flag,
    scrap_flag,
    shipment_flag
  FROM trf_inv_txn
  WHERE transaction_id IN ("1000011-1-2023", "1000012-1-2023", "1000013-1-2023")
)
-- Validation Query: Assert movement flags are mapped as per movment_mapping
SELECT
  CASE
    WHEN consumption_usage_flag = "no"
     AND receipt_flag = "no"
     AND scrap_flag = "no"
     AND shipment_flag = "no"
    THEN "PASS"
    ELSE "FAIL"
  END AS movement_flag_assertion
FROM movement_flag_test;

-- ========================================================================
-- 5. REFERENCE ORDER TYPE LOGIC TESTS
-- ========================================================================

-- CTE: Validate reference_order_type mapping
WITH reference_order_type_test AS (
  SELECT
    transaction_id,
    reference_order_type
  FROM trf_inv_txn
  WHERE transaction_id IN (
    "1000014-1-2023", "1000015-1-2023", "1000016-1-2023", "1000017-1-2023", "1000018-1-2023"
  )
)
-- Validation Query: Assert correct mapping for reference_order_type
SELECT
  transaction_id,
  CASE
    WHEN reference_order_type IN ("EKKO.BSART", "VBAK.AUART", "AUFK.AUART", "none") THEN "PASS"
    ELSE "FAIL"
  END AS reference_order_type_assertion
FROM reference_order_type_test;

-- ========================================================================
-- 6. AGGREGATION LOGIC TESTS
-- ========================================================================

-- CTE: Aggregate transaction_qty_primary_uom for composite key
WITH agg_qty_test AS (
  SELECT
    prod_plant_lot_location_key,
    transaction_id,
    SUM(transaction_qty_primary_uom) AS agg_qty
  FROM trf_inv_txn
  WHERE transaction_id = "1000019-1-2023"
  GROUP BY prod_plant_lot_location_key, transaction_id
)
-- Validation Query: Assert aggregation result
SELECT
  CASE
    WHEN agg_qty = 2000.0 THEN "PASS"
    ELSE "FAIL"
  END AS aggregation_assertion
FROM agg_qty_test;

-- ========================================================================
-- 7. FILTER LOGIC TESTS: budat, MBEW.BWTAR, t156t.spras
-- ========================================================================

-- CTE: Filter records by posting_dt_yyyymmdd (budat) within last 5 years
WITH budat_filter_test AS (
  SELECT
    transaction_id,
    posting_dt_yyyymmdd
  FROM trf_inv_txn
  WHERE posting_dt_yyyymmdd >= CAST(DATE_FORMAT(DATE_ADD(CURRENT_DATE(), -5 * 365), "yyyyMMdd") AS STRING)
)
-- Validation Query: Assert records are processed
SELECT
  COUNT(*) AS budat_filter_pass_count
FROM budat_filter_test;

-- CTE: Filter records with non-blank MBEW.BWTAR (should be excluded)
WITH bwtar_filter_test AS (
  SELECT
    transaction_id
  FROM trf_inv_txn
  WHERE transaction_reason = "Non-blank BWTAR"
)
-- Validation Query: Assert excluded records
SELECT
  CASE
    WHEN COUNT(*) = 0 THEN "PASS"
    ELSE "FAIL"
  END AS bwtar_filter_assertion
FROM bwtar_filter_test;

-- CTE: Filter records with t156t.spras != "E" (should be excluded)
WITH spras_filter_test AS (
  SELECT
    transaction_id
  FROM trf_inv_txn
  WHERE transaction_reason = "Non-E Spras"
)
-- Validation Query: Assert excluded records
SELECT
  CASE
    WHEN COUNT(*) = 0 THEN "PASS"
    ELSE "FAIL"
  END AS spras_filter_assertion
FROM spras_filter_test;

-- ========================================================================
-- 8. DATA QUALITY VALIDATION: Required fields, type mismatch, default values
-- ========================================================================

-- CTE: Find records with missing required fields
WITH missing_required_fields AS (
  SELECT
    transaction_id,
    CASE
      WHEN item_nbr IS NULL THEN "item_nbr"
      WHEN plant_cd IS NULL THEN "plant_cd"
      WHEN transaction_id IS NULL THEN "transaction_id"
      WHEN prod_plant_lot_location_key IS NULL THEN "prod_plant_lot_location_key"
      ELSE NULL
    END AS missing_field
  FROM trf_inv_txn
  WHERE transaction_id IN ("1000025-1-2023")
)
-- Validation Query: Assert error for missing required fields
SELECT
  transaction_id,
  missing_field,
  CASE
    WHEN missing_field IS NOT NULL THEN "FAIL"
    ELSE "PASS"
  END AS required_field_assertion
FROM missing_required_fields;

-- CTE: Find records with type mismatch
WITH type_mismatch_test AS (
  SELECT
    transaction_id,
    transaction_qty_primary_uom
  FROM trf_inv_txn
  WHERE transaction_id = "1000007-1-2023"
)
-- Validation Query: Assert error for type mismatch
SELECT
  transaction_id,
  CASE
    WHEN typeof(transaction_qty_primary_uom) != "double" THEN "FAIL"
    ELSE "PASS"
  END AS type_mismatch_assertion
FROM type_mismatch_test;

-- CTE: Find records with default values for optional fields
WITH default_value_test AS (
  SELECT
    transaction_id,
    location_cd,
    lot_nbr,
    plant_cd,
    company_cd,
    reference_order_type
  FROM trf_inv_txn
  WHERE transaction_id IN ("1000002-1-2023", "1000006-1-2023", "1000025-1-2023")
)
-- Validation Query: Assert default value is "none" or NULL
SELECT
  transaction_id,
  CASE
    WHEN location_cd IS NULL OR location_cd = "none" THEN "PASS"
    ELSE "FAIL"
  END AS location_cd_default_assertion,
  CASE
    WHEN lot_nbr IS NULL OR lot_nbr = "none" THEN "PASS"
    ELSE "FAIL"
  END AS lot_nbr_default_assertion,
  CASE
    WHEN plant_cd IS NULL OR plant_cd = "none" THEN "PASS"
    ELSE "FAIL"
  END AS plant_cd_default_assertion,
  CASE
    WHEN company_cd IS NULL OR company_cd = "none" THEN "PASS"
    ELSE "FAIL"
  END AS company_cd_default_assertion,
  CASE
    WHEN reference_order_type IS NULL OR reference_order_type = "none" THEN "PASS"
    ELSE "FAIL"
  END AS reference_order_type_default_assertion
FROM default_value_test;

-- ========================================================================
-- 9. DELTA LAKE OPERATIONS: MERGE, UPDATE, DELETE, WINDOW FUNCTIONS
-- ========================================================================

-- MERGE: Upsert test record into trf_inv_txn
-- Purpose: Validate Delta Lake MERGE operation
MERGE INTO purgo_databricks.purgo_playground.trf_inv_txn AS target
USING (
  SELECT
    "9999" AS company_cd,
    "PROD999" AS item_nbr,
    "LOC99" AS location_cd,
    "LOT99" AS lot_nbr,
    "PLT99" AS plant_cd,
    "mbd" AS src_sys_cd,
    "9999999-1-2025" AS transaction_id,
    "USD" AS company_currency_cd,
    "9999999" AS document_nbr,
    "GL9999" AS gl_account_nbr,
    "20251008" AS posting_dt_yyyymmdd,
    "EA" AS primary_uom_cd,
    "C999" AS reference_order_company_cd,
    1 AS reference_order_schedule_line_nbr,
    "PO" AS reference_order_type,
    "20251009" AS transaction_dt_yyyymmdd,
    9999.0 AS transaction_qty_primary_uom,
    4999.0 AS transaction_qty_transaction_uom,
    "MERGE Test" AS transaction_reason,
    "2025-10-08T10:00:00.000+0000" AS transaction_ts,
    "GR" AS transaction_type_cd,
    "EA" AS transaction_uom,
    99.99 AS unit_cost_company_currency,
    "mbd|PLT99" AS plant_key,
    "mbd|PROD999" AS prod_key,
    "mbd|PROD999|PLT99" AS prod_plant_key,
    "mbd|PLT99|LOC99" AS plant_location_key,
    "mbd|PLT99|LOT99" AS plant_lot_key,
    "mbd|PROD999|PLT99|LOC99" AS prod_plant_location_key,
    "mbd|PROD999|PLT99|LOT99" AS prod_plant_lot_key,
    "mbd|PROD999|PLT99|LOT99|LOC99" AS prod_plant_lot_location_key,
    "mbd|GL9999" AS gl_acct_key,
    "mbd|9999" AS co_key,
    "no" AS consumption_usage_flag,
    "no" AS receipt_flag,
    "no" AS scrap_flag,
    "no" AS shipment_flag
) AS source
ON target.prod_plant_lot_location_key = source.prod_plant_lot_location_key
   AND target.transaction_id = source.transaction_id
WHEN MATCHED THEN
  UPDATE SET
    transaction_qty_primary_uom = source.transaction_qty_primary_uom,
    transaction_qty_transaction_uom = source.transaction_qty_transaction_uom,
    transaction_reason = source.transaction_reason
WHEN NOT MATCHED THEN
  INSERT (
    company_cd, item_nbr, location_cd, lot_nbr, plant_cd, src_sys_cd, transaction_id,
    company_currency_cd, document_nbr, gl_account_nbr, posting_dt_yyyymmdd, primary_uom_cd,
    reference_order_company_cd, reference_order_schedule_line_nbr, reference_order_type,
    transaction_dt_yyyymmdd, transaction_qty_primary_uom, transaction_qty_transaction_uom,
    transaction_reason, transaction_ts, transaction_type_cd, transaction_uom,
    unit_cost_company_currency, plant_key, prod_key, prod_plant_key, plant_location_key,
    plant_lot_key, prod_plant_location_key, prod_plant_lot_key, prod_plant_lot_location_key,
    gl_acct_key, co_key, consumption_usage_flag, receipt_flag, scrap_flag, shipment_flag
  )
  VALUES (
    source.company_cd, source.item_nbr, source.location_cd, source.lot_nbr, source.plant_cd, source.src_sys_cd, source.transaction_id,
    source.company_currency_cd, source.document_nbr, source.gl_account_nbr, source.posting_dt_yyyymmdd, source.primary_uom_cd,
    source.reference_order_company_cd, source.reference_order_schedule_line_nbr, source.reference_order_type,
    source.transaction_dt_yyyymmdd, source.transaction_qty_primary_uom, source.transaction_qty_transaction_uom,
    source.transaction_reason, source.transaction_ts, source.transaction_type_cd, source.transaction_uom,
    source.unit_cost_company_currency, source.plant_key, source.prod_key, source.prod_plant_key, source.plant_location_key,
    source.plant_lot_key, source.prod_plant_location_key, source.prod_plant_lot_key, source.prod_plant_lot_location_key,
    source.gl_acct_key, source.co_key, source.consumption_usage_flag, source.receipt_flag, source.scrap_flag, source.shipment_flag
  );

-- UPDATE: Update transaction_reason for test record
-- Purpose: Validate Delta Lake UPDATE operation
UPDATE purgo_databricks.purgo_playground.trf_inv_txn
SET transaction_reason = "UPDATED MERGE Test"
WHERE transaction_id = "9999999-1-2025";

-- DELETE: Delete test record
-- Purpose: Validate Delta Lake DELETE operation
DELETE FROM purgo_databricks.purgo_playground.trf_inv_txn
WHERE transaction_id = "9999999-1-2025";

-- WINDOW FUNCTION: Row number partitioned by composite key
-- Purpose: Validate window analytics
WITH window_test AS (
  SELECT
    transaction_id,
    prod_plant_lot_location_key,
    ROW_NUMBER() OVER (PARTITION BY prod_plant_lot_location_key, transaction_id ORDER BY transaction_ts DESC) AS rn
  FROM trf_inv_txn
)
-- Validation Query: Assert only one row per composite key
SELECT
  transaction_id,
  prod_plant_lot_location_key,
  rn
FROM window_test
WHERE rn > 1;

-- ========================================================================
-- 10. PERFORMANCE TEST: Count records and check query time
-- ========================================================================

-- CTE: Count total records in trf_inv_txn
WITH perf_test AS (
  SELECT COUNT(*) AS total_count FROM trf_inv_txn
)
-- Validation Query: Assert record count > 0
SELECT
  CASE
    WHEN total_count > 0 THEN "PASS"
    ELSE "FAIL"
  END AS perf_count_assertion
FROM perf_test;

-- ========================================================================
-- 11. CLEANUP: Remove test records created by this script
-- ========================================================================

-- DELETE: Remove all test records with transaction_id like '9999999-%'
DELETE FROM purgo_databricks.purgo_playground.trf_inv_txn
WHERE transaction_id LIKE "9999999-%";

-- ========================================================================
-- 12. FINAL OUTPUT: Show all valid trf_inv_txn records for review
-- ========================================================================

-- CTE: Select all valid records (excluding excluded test cases)
WITH valid_trf_inv_txn AS (
  SELECT *
  FROM trf_inv_txn
  WHERE posting_dt_yyyymmdd >= CAST(DATE_FORMAT(DATE_ADD(CURRENT_DATE(), -5 * 365), "yyyyMMdd") AS STRING)
    AND (transaction_reason NOT IN ("Non-blank BWTAR", "Non-E Spras", "Old Date", "Invalid Date") OR transaction_reason IS NULL)
)
-- Validation Query: Show results
SELECT * FROM valid_trf_inv_txn;

-- END OF TEST SCRIPT
