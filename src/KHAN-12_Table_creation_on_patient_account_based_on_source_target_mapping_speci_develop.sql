/* 
    Databricks SQL Implementation: Creation of patient_account Table from pa_account Source
    Catalog: purgo_databricks
    Schema: purgo_playground
    Table: patient_account
    Author: Production Implementation
    ---------------------------------------------------------------------------
    This script:
      - Drops patient_account table if it exists
      - Creates patient_account table with required schema and column comments
      - Populates patient_account from pa_account and other sources as needed
      - Enforces all validation and business rules via SQL logic
      - Documents all columns and logic
      - Uses CTE for transformation and validation
      - Ensures schema consistency and error handling
    ---------------------------------------------------------------------------
*/

/* ---------------------------------------------------------------------------
   SECTION: Setup - Drop and Create Table
--------------------------------------------------------------------------- */

USE CATALOG purgo_databricks;

-- Drop patient_account table if it already exists
DROP TABLE IF EXISTS purgo_databricks.purgo_playground.patient_account;

-- Create patient_account table with required schema and column comments
CREATE TABLE purgo_databricks.purgo_playground.patient_account (
    patient_foundation_shipment STRING NOT NULL COMMENT 'Patient foundation shipment identifier. Must not be null or empty.',
    prescriber_id STRING NOT NULL COMMENT 'Unique prescriber identifier. Format: /^[A-Za-z0-9_-]{6,32}$/',
    prescriber_key STRING NOT NULL COMMENT 'Prescriber key for internal mapping. Must be unique per prescriber_id.',
    patient_sf_id STRING NOT NULL COMMENT 'Salesforce patient ID. Format: /^[A-Za-z0-9]{8,18}$/',
    service_request_type STRING NOT NULL COMMENT 'Type of service request. Valid values: NEW, UPDATE, CLOSE.',
    case_sf_id STRING NOT NULL COMMENT 'Salesforce case ID. Format: /^[A-Za-z0-9]{8,18}$/',
    account_id STRING NOT NULL COMMENT 'Account identifier. Format: /^[A-Za-z0-9]{8,18}$/',
    hash_key STRING NOT NULL COMMENT 'Hash key for record uniqueness. SHA256 of concatenated key fields.',
    last_modified_date TIMESTAMP NOT NULL COMMENT 'Last modified timestamp. Must be valid timestamp.',
    planned_date DATE COMMENT 'Planned date for patient account. Optional, must be valid date if present.'
)
COMMENT 'Centralized patient account information for healthcare management';

/* ---------------------------------------------------------------------------
   SECTION: Data Transformation and Validation CTE
--------------------------------------------------------------------------- */

WITH validated_source AS (
    SELECT
        -- Source columns from pa_account
        pa.patient_foundation_shipment,
        -- Derive prescriber_id: use prescriber_name_c, fallback to NULL if missing
        pa.prescriber_name_c AS prescriber_id,
        -- Derive prescriber_key: use prescriber_name_c_address, fallback to NULL if missing
        pa.prescriber_name_c_address AS prescriber_key,
        -- Derive patient_sf_id: use patinet_c, fallback to NULL if missing
        pa.patinet_c AS patient_sf_id,
        -- Derive service_request_type: use recordtypeid, fallback to NULL if missing
        pa.recordtypeid AS service_request_type,
        -- Derive case_sf_id: use id, fallback to NULL if missing
        pa.id AS case_sf_id,
        -- Derive account_id: use accountid, fallback to NULL if missing
        pa.accountid AS account_id,
        -- Hash key: SHA256 of concatenated key fields
        sha2(concat(
            coalesce(pa.patient_foundation_shipment, ''),
            '|',
            coalesce(pa.prescriber_name_c, ''),
            '|',
            coalesce(pa.patinet_c, ''),
            '|',
            coalesce(pa.accountid, '')
        ), 256) AS hash_key,
        -- last_modified_date: use current_timestamp as default
        current_timestamp() AS last_modified_date,
        -- planned_date: NULL (no source mapping)
        NULL AS planned_date,
        -- Validation flags
        CASE WHEN pa.patient_foundation_shipment IS NULL OR trim(pa.patient_foundation_shipment) = '' THEN 1 ELSE 0 END AS err_patient_foundation_shipment,
        CASE WHEN pa.prescriber_name_c IS NULL OR trim(pa.prescriber_name_c) = '' THEN 1
             WHEN NOT (pa.prescriber_name_c RLIKE '^[A-Za-z0-9_-]{6,32}$') THEN 2 ELSE 0 END AS err_prescriber_id,
        CASE WHEN pa.prescriber_name_c_address IS NULL OR trim(pa.prescriber_name_c_address) = '' THEN 1 ELSE 0 END AS err_prescriber_key,
        CASE WHEN pa.patinet_c IS NULL OR trim(pa.patinet_c) = '' THEN 1
             WHEN NOT (pa.patinet_c RLIKE '^[A-Za-z0-9]{8,18}$') THEN 2 ELSE 0 END AS err_patient_sf_id,
        CASE WHEN pa.recordtypeid IS NULL OR trim(pa.recordtypeid) = '' THEN 1
             WHEN pa.recordtypeid NOT IN ('NEW', 'UPDATE', 'CLOSE') THEN 2 ELSE 0 END AS err_service_request_type,
        CASE WHEN pa.id IS NULL OR trim(pa.id) = '' THEN 1
             WHEN NOT (pa.id RLIKE '^[A-Za-z0-9]{8,18}$') THEN 2 ELSE 0 END AS err_case_sf_id,
        CASE WHEN pa.accountid IS NULL OR trim(pa.accountid) = '' THEN 1
             WHEN NOT (pa.accountid RLIKE '^[A-Za-z0-9]{8,18}$') THEN 2 ELSE 0 END AS err_account_id
    FROM purgo_databricks.purgo_playground.pa_account pa
)
, error_flags AS (
    SELECT
        *,
        -- Aggregate error flag
        CASE
            WHEN err_patient_foundation_shipment = 1 THEN 'Field patient_foundation_shipment is required and cannot be null/empty'
            WHEN err_prescriber_id = 1 THEN 'Field prescriber_id is required and cannot be null/empty'
            WHEN err_prescriber_id = 2 THEN 'Invalid prescriber_id format'
            WHEN err_prescriber_key = 1 THEN 'Field prescriber_key is required and cannot be null/empty'
            WHEN err_patient_sf_id = 1 THEN 'Field patient_sf_id is required and cannot be null/empty'
            WHEN err_patient_sf_id = 2 THEN 'Invalid patient_sf_id format'
            WHEN err_service_request_type = 1 THEN 'Field service_request_type is required and cannot be null/empty'
            WHEN err_service_request_type = 2 THEN 'Invalid service_request_type value'
            WHEN err_case_sf_id = 1 THEN 'Field case_sf_id is required and cannot be null/empty'
            WHEN err_case_sf_id = 2 THEN 'Invalid case_sf_id format'
            WHEN err_account_id = 1 THEN 'Field account_id is required and cannot be null/empty'
            WHEN err_account_id = 2 THEN 'Invalid account_id format'
            ELSE NULL
        END AS error_message
    FROM validated_source
)
, deduped AS (
    -- Enforce uniqueness of prescriber_key per prescriber_id
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY prescriber_id, prescriber_key) AS prescriber_key_count
    FROM error_flags
)
, final_validated AS (
    SELECT
        patient_foundation_shipment,
        prescriber_id,
        prescriber_key,
        patient_sf_id,
        service_request_type,
        case_sf_id,
        account_id,
        hash_key,
        last_modified_date,
        planned_date
    FROM deduped
    WHERE error_message IS NULL
      AND prescriber_key_count = 1
)

/* ---------------------------------------------------------------------------
   SECTION: Insert Validated Data into patient_account
--------------------------------------------------------------------------- */

INSERT INTO purgo_databricks.purgo_playground.patient_account (
    patient_foundation_shipment,
    prescriber_id,
    prescriber_key,
    patient_sf_id,
    service_request_type,
    case_sf_id,
    account_id,
    hash_key,
    last_modified_date,
    planned_date
)
SELECT
    patient_foundation_shipment,
    prescriber_id,
    prescriber_key,
    patient_sf_id,
    service_request_type,
    case_sf_id,
    account_id,
    hash_key,
    last_modified_date,
    planned_date
FROM final_validated;

/* ---------------------------------------------------------------------------
   SECTION: Validation Query - Show Invalid Records and Error Messages
--------------------------------------------------------------------------- */

WITH validation_cte AS (
    SELECT
        patient_foundation_shipment,
        prescriber_id,
        prescriber_key,
        patient_sf_id,
        service_request_type,
        case_sf_id,
        account_id,
        hash_key,
        last_modified_date,
        planned_date,
        error_message,
        prescriber_key_count
    FROM deduped
    WHERE error_message IS NOT NULL OR prescriber_key_count > 1
)
SELECT
    patient_foundation_shipment AS patient_foundation_shipment,
    prescriber_id AS prescriber_id,
    prescriber_key AS prescriber_key,
    patient_sf_id AS patient_sf_id,
    service_request_type AS service_request_type,
    case_sf_id AS case_sf_id,
    account_id AS account_id,
    hash_key AS hash_key,
    last_modified_date AS last_modified_date,
    planned_date AS planned_date,
    error_message AS error_message
FROM validation_cte
ORDER BY patient_foundation_shipment;

/* ---------------------------------------------------------------------------
   SECTION: End of Script
--------------------------------------------------------------------------- */
-- End of production implementation for patient_account table creation and population

