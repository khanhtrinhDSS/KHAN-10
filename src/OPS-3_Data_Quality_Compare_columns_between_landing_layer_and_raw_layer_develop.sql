-- Databricks SQL script: Site Data Ingestion and Validation for abc1234-1
-- Purpose: Validate and ingest daily site data from landing to raw and raw_reject layers
-- Author: Khanh Trinh
-- Date: 2025-10-10
-- Description: This script processes site data from purgo_databricks.purgo_playground.landing_abc1234_1, moving valid records to raw_abc1234_1 and invalid/mismatched records to raw_reject_abc1234_1 per business rules

USE CATALOG purgo_databricks;

-- ==========================================================================================
-- Step 1: Insert invalid records from landing_abc1234_1 into raw_reject_abc1234_1
-- ==========================================================================================
INSERT INTO purgo_playground.raw_reject_abc1234_1 (
  Clinical_Study_Source_Id,
  Study_Country_source_id,
  Study_Site_source_id,
  raw_site_number,
  landing_site_number,
  error_message
)
WITH landing_validation_invalid AS (
  SELECT
    COALESCE(CAST(Clinical_Study_Source_Id AS STRING), '') AS Clinical_Study_Source_Id,
    COALESCE(CAST(Study_Country_source_id AS STRING), '') AS Study_Country_source_id,
    COALESCE(CAST(Study_Site_source_id AS STRING), '') AS Study_Site_source_id,
    site_number,
    site_status,
    primary_investigator_source_id,
    CASE
      WHEN Clinical_Study_Source_Id IS NULL OR Clinical_Study_Source_Id = '' THEN 'Clinical_Study_Source_Id is required'
      WHEN Study_Country_source_id IS NULL OR Study_Country_source_id = '' THEN 'Study_Country_source_id is required'
      WHEN Study_Site_source_id IS NULL OR Study_Site_source_id = '' THEN 'Study_Site_source_id is required'
      WHEN site_number IS NULL THEN 'site_number is required'
      WHEN CAST(site_number AS STRING) RLIKE '^[0-9]+$' = FALSE THEN 'site_number must be a bigint'
      ELSE ''
    END AS error_message,
    CASE
      WHEN Clinical_Study_Source_Id IS NULL OR Clinical_Study_Source_Id = '' THEN FALSE
      WHEN Study_Country_source_id IS NULL OR Study_Country_source_id = '' THEN FALSE
      WHEN Study_Site_source_id IS NULL OR Study_Site_source_id = '' THEN FALSE
      WHEN site_number IS NULL THEN FALSE
      WHEN CAST(site_number AS STRING) RLIKE '^[0-9]+$' = FALSE THEN FALSE
      ELSE TRUE
    END AS valid
  FROM purgo_playground.landing_abc1234_1
)
SELECT
  Clinical_Study_Source_Id,
  Study_Country_source_id,
  Study_Site_source_id,
  NULL AS raw_site_number,
  CAST(site_number AS BIGINT) AS landing_site_number,
  error_message
FROM landing_validation_invalid
WHERE valid = FALSE;

-- ==========================================================================================
-- Step 2: Insert valid records (new or matching site_number) into raw_abc1234_1
-- ==========================================================================================
INSERT INTO purgo_playground.raw_abc1234_1 (
  Clinical_Study_Source_Id,
  Study_Country_source_id,
  Study_Site_source_id,
  site_number,
  site_status,
  primary_investigator_source_id
)
WITH landing_validation_valid AS (
  SELECT
    COALESCE(CAST(Clinical_Study_Source_Id AS STRING), '') AS Clinical_Study_Source_Id,
    COALESCE(CAST(Study_Country_source_id AS STRING), '') AS Study_Country_source_id,
    COALESCE(CAST(Study_Site_source_id AS STRING), '') AS Study_Site_source_id,
    CAST(site_number AS BIGINT) AS site_number,
    COALESCE(CAST(site_status AS STRING), '') AS site_status,
    COALESCE(CAST(primary_investigator_source_id AS STRING), '') AS primary_investigator_source_id,
    CASE
      WHEN Clinical_Study_Source_Id IS NULL OR Clinical_Study_Source_Id = '' THEN 'Clinical_Study_Source_Id is required'
      WHEN Study_Country_source_id IS NULL OR Study_Country_source_id = '' THEN 'Study_Country_source_id is required'
      WHEN Study_Site_source_id IS NULL OR Study_Site_source_id = '' THEN 'Study_Site_source_id is required'
      WHEN site_number IS NULL THEN 'site_number is required'
      WHEN CAST(site_number AS STRING) RLIKE '^[0-9]+$' = FALSE THEN 'site_number must be a bigint'
      ELSE ''
    END AS error_message,
    CASE
      WHEN Clinical_Study_Source_Id IS NULL OR Clinical_Study_Source_Id = '' THEN FALSE
      WHEN Study_Country_source_id IS NULL OR Study_Country_source_id = '' THEN FALSE
      WHEN Study_Site_source_id IS NULL OR Study_Site_source_id = '' THEN FALSE
      WHEN site_number IS NULL THEN FALSE
      WHEN CAST(site_number AS STRING) RLIKE '^[0-9]+$' = FALSE THEN FALSE
      ELSE TRUE
    END AS valid
  FROM purgo_playground.landing_abc1234_1
),
raw_keys AS (
  SELECT
    Clinical_Study_Source_Id,
    Study_Country_source_id,
    Study_Site_source_id,
    site_number
  FROM purgo_playground.raw_abc1234_1
),
to_raw AS (
  SELECT
    v.Clinical_Study_Source_Id,
    v.Study_Country_source_id,
    v.Study_Site_source_id,
    v.site_number,
    v.site_status,
    v.primary_investigator_source_id
  FROM landing_validation_valid v
  LEFT JOIN raw_keys r
    ON v.Clinical_Study_Source_Id = r.Clinical_Study_Source_Id
   AND v.Study_Country_source_id = r.Study_Country_source_id
   AND v.Study_Site_source_id = r.Study_Site_source_id
  WHERE v.valid = TRUE
    AND (
      r.Clinical_Study_Source_Id IS NULL
      OR v.site_number = r.site_number
    )
)
SELECT
  Clinical_Study_Source_Id,
  Study_Country_source_id,
  Study_Site_source_id,
  site_number,
  site_status,
  primary_investigator_source_id
FROM to_raw;

-- ==========================================================================================
-- Step 3: Insert records with site_number mismatch into raw_reject_abc1234_1
-- ==========================================================================================
INSERT INTO purgo_playground.raw_reject_abc1234_1 (
  Clinical_Study_Source_Id,
  Study_Country_source_id,
  Study_Site_source_id,
  raw_site_number,
  landing_site_number,
  error_message
)
WITH landing_validation_valid AS (
  SELECT
    COALESCE(CAST(Clinical_Study_Source_Id AS STRING), '') AS Clinical_Study_Source_Id,
    COALESCE(CAST(Study_Country_source_id AS STRING), '') AS Study_Country_source_id,
    COALESCE(CAST(Study_Site_source_id AS STRING), '') AS Study_Site_source_id,
    CAST(site_number AS BIGINT) AS site_number,
    COALESCE(CAST(site_status AS STRING), '') AS site_status,
    COALESCE(CAST(primary_investigator_source_id AS STRING), '') AS primary_investigator_source_id,
    CASE
      WHEN Clinical_Study_Source_Id IS NULL OR Clinical_Study_Source_Id = '' THEN 'Clinical_Study_Source_Id is required'
      WHEN Study_Country_source_id IS NULL OR Study_Country_source_id = '' THEN 'Study_Country_source_id is required'
      WHEN Study_Site_source_id IS NULL OR Study_Site_source_id = '' THEN 'Study_Site_source_id is required'
      WHEN site_number IS NULL THEN 'site_number is required'
      WHEN CAST(site_number AS STRING) RLIKE '^[0-9]+$' = FALSE THEN 'site_number must be a bigint'
      ELSE ''
    END AS error_message,
    CASE
      WHEN Clinical_Study_Source_Id IS NULL OR Clinical_Study_Source_Id = '' THEN FALSE
      WHEN Study_Country_source_id IS NULL OR Study_Country_source_id = '' THEN FALSE
      WHEN Study_Site_source_id IS NULL OR Study_Site_source_id = '' THEN FALSE
      WHEN site_number IS NULL THEN FALSE
      WHEN CAST(site_number AS STRING) RLIKE '^[0-9]+$' = FALSE THEN FALSE
      ELSE TRUE
    END AS valid
  FROM purgo_playground.landing_abc1234_1
),
raw_keys AS (
  SELECT
    Clinical_Study_Source_Id,
    Study_Country_source_id,
    Study_Site_source_id,
    site_number
  FROM purgo_playground.raw_abc1234_1
),
to_reject AS (
  SELECT
    v.Clinical_Study_Source_Id,
    v.Study_Country_source_id,
    v.Study_Site_source_id,
    r.site_number AS raw_site_number,
    v.site_number AS landing_site_number,
    'Site number mismatch between landing and raw for given study/site/country combination' AS error_message
  FROM landing_validation_valid v
  INNER JOIN raw_keys r
    ON v.Clinical_Study_Source_Id = r.Clinical_Study_Source_Id
   AND v.Study_Country_source_id = r.Study_Country_source_id
   AND v.Study_Site_source_id = r.Study_Site_source_id
  WHERE v.valid = TRUE
    AND v.site_number != r.site_number
)
SELECT
  Clinical_Study_Source_Id,
  Study_Country_source_id,
  Study_Site_source_id,
  raw_site_number,
  landing_site_number,
  error_message
FROM to_reject;
