USE CATALOG purgo_databricks;

-- Drop the patient_account table if it already exists
DROP TABLE IF EXISTS purgo_databricks.purgo_playground.patient_account;

-- Create the patient_account table with required schema and column comments
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
    planned_date DATE COMMENT 'Planned date for patient account'
)
COMMENT 'Centralized patient account information for healthcare management';

-- Test data generation using CTE for diverse scenarios
WITH test_patient_account_data AS (
    SELECT
        -- Happy path: all valid fields
        'SHIP123456' AS patient_foundation_shipment,
        'PRSC7890' AS prescriber_id,
        'KEY001' AS prescriber_key,
        'PAT12345678' AS patient_sf_id,
        'NEW' AS service_request_type,
        'CASE876543' AS case_sf_id,
        'ACC1234567' AS account_id,
        sha2(concat('SHIP123456', '|', 'PRSC7890', '|', 'PAT12345678', '|', 'ACC1234567'), 256) AS hash_key,
        CAST('2024-06-01T12:34:56.000+0000' AS TIMESTAMP) AS last_modified_date,
        CAST('2024-06-15' AS DATE) AS planned_date
    UNION ALL
    -- Happy path: planned_date is null
    SELECT
        'SHIP654321',
        'PRSC1234',
        'KEY002',
        'PAT87654321',
        'UPDATE',
        'CASE123456',
        'ACC7654321',
        sha2(concat('SHIP654321', '|', 'PRSC1234', '|', 'PAT87654321', '|', 'ACC7654321'), 256),
        CAST('2024-06-02T08:00:00.000+0000' AS TIMESTAMP),
        NULL
    UNION ALL
    -- Edge case: prescriber_id at minimum length (6 chars)
    SELECT
        'SHIPMINLEN',
        'PRSC01',
        'KEY003',
        'PATMINLEN01',
        'CLOSE',
        'CASEMINLEN',
        'ACC000001',
        sha2(concat('SHIPMINLEN', '|', 'PRSC01', '|', 'PATMINLEN01', '|', 'ACC000001'), 256),
        CAST('2024-06-03T00:00:00.000+0000' AS TIMESTAMP),
        CAST('2024-06-20' AS DATE)
    UNION ALL
    -- Edge case: prescriber_id at maximum length (32 chars)
    SELECT
        'SHIPMAXLEN',
        'PRSC012345678901234567890123456789',
        'KEY004',
        'PATMAXLEN01',
        'NEW',
        'CASEMAXLEN',
        'ACC9999999',
        sha2(concat('SHIPMAXLEN', '|', 'PRSC012345678901234567890123456789', '|', 'PATMAXLEN01', '|', 'ACC9999999'), 256),
        CAST('2024-06-04T23:59:59.999+0000' AS TIMESTAMP),
        CAST('2024-06-21' AS DATE)
    UNION ALL
    -- Edge case: patient_sf_id at minimum length (8 chars)
    SELECT
        'SHIPEDGE01',
        'PRSCEDGE',
        'KEY005',
        'PATEDGE1',
        'UPDATE',
        'CASEEDGE01',
        'ACCEDGE01',
        sha2(concat('SHIPEDGE01', '|', 'PRSCEDGE', '|', 'PATEDGE1', '|', 'ACCEDGE01'), 256),
        CAST('2024-06-05T10:10:10.000+0000' AS TIMESTAMP),
        NULL
    UNION ALL
    -- Edge case: patient_sf_id at maximum length (18 chars)
    SELECT
        'SHIPEDGE02',
        'PRSCEDGE2',
        'KEY006',
        'PATEDGE12345678901',
        'CLOSE',
        'CASEEDGE02',
        'ACCEDGE02',
        sha2(concat('SHIPEDGE02', '|', 'PRSCEDGE2', '|', 'PATEDGE12345678901', '|', 'ACCEDGE02'), 256),
        CAST('2024-06-06T11:11:11.000+0000' AS TIMESTAMP),
        CAST('2024-06-22' AS DATE)
    UNION ALL
    -- Error case: invalid prescriber_id (special chars)
    SELECT
        'SHIPERR01',
        'PRSC!@#',
        'KEY007',
        'PATERR01',
        'NEW',
        'CASEERR01',
        'ACCERR01',
        sha2(concat('SHIPERR01', '|', 'PRSC!@#', '|', 'PATERR01', '|', 'ACCERR01'), 256),
        CAST('2024-06-07T12:00:00.000+0000' AS TIMESTAMP),
        CAST('2024-06-23' AS DATE)
    UNION ALL
    -- Error case: invalid patient_sf_id (special chars)
    SELECT
        'SHIPERR02',
        'PRSCERR02',
        'KEY008',
        'PAT!@#',
        'UPDATE',
        'CASEERR02',
        'ACCERR02',
        sha2(concat('SHIPERR02', '|', 'PRSCERR02', '|', 'PAT!@#', '|', 'ACCERR02'), 256),
        CAST('2024-06-08T13:00:00.000+0000' AS TIMESTAMP),
        NULL
    UNION ALL
    -- Error case: invalid service_request_type
    SELECT
        'SHIPERR03',
        'PRSCERR03',
        'KEY009',
        'PATERR03',
        'INVALID',
        'CASEERR03',
        'ACCERR03',
        sha2(concat('SHIPERR03', '|', 'PRSCERR03', '|', 'PATERR03', '|', 'ACCERR03'), 256),
        CAST('2024-06-09T14:00:00.000+0000' AS TIMESTAMP),
        CAST('2024-06-24' AS DATE)
    UNION ALL
    -- Error case: invalid account_id (special chars)
    SELECT
        'SHIPERR04',
        'PRSCERR04',
        'KEY010',
        'PATERR04',
        'NEW',
        'CASEERR04',
        'ACC!@#',
        sha2(concat('SHIPERR04', '|', 'PRSCERR04', '|', 'PATERR04', '|', 'ACC!@#'), 256),
        CAST('2024-06-10T15:00:00.000+0000' AS TIMESTAMP),
        CAST('2024-06-25' AS DATE)
    UNION ALL
    -- Error case: invalid last_modified_date (not a timestamp)
    SELECT
        'SHIPERR05',
        'PRSCERR05',
        'KEY011',
        'PATERR05',
        'UPDATE',
        'CASEERR05',
        'ACCERR05',
        sha2(concat('SHIPERR05', '|', 'PRSCERR05', '|', 'PATERR05', '|', 'ACCERR05'), 256),
        CAST(NULL AS TIMESTAMP),
        NULL
    UNION ALL
    -- Error case: invalid planned_date (not a date)
    SELECT
        'SHIPERR06',
        'PRSCERR06',
        'KEY012',
        'PATERR06',
        'CLOSE',
        'CASEERR06',
        'ACCERR06',
        sha2(concat('SHIPERR06', '|', 'PRSCERR06', '|', 'PATERR06', '|', 'ACCERR06'), 256),
        CAST('2024-06-11T16:00:00.000+0000' AS TIMESTAMP),
        CAST(NULL AS DATE)
    UNION ALL
    -- Error case: NULL required field (patient_foundation_shipment)
    SELECT
        NULL,
        'PRSCERR07',
        'KEY013',
        'PATERR07',
        'NEW',
        'CASEERR07',
        'ACCERR07',
        sha2(concat('', '|', 'PRSCERR07', '|', 'PATERR07', '|', 'ACCERR07'), 256),
        CAST('2024-06-12T17:00:00.000+0000' AS TIMESTAMP),
        CAST('2024-06-26' AS DATE)
    UNION ALL
    -- Error case: NULL required field (prescriber_id)
    SELECT
        'SHIPERR08',
        NULL,
        'KEY014',
        'PATERR08',
        'UPDATE',
        'CASEERR08',
        'ACCERR08',
        sha2(concat('SHIPERR08', '|', '', '|', 'PATERR08', '|', 'ACCERR08'), 256),
        CAST('2024-06-13T18:00:00.000+0000' AS TIMESTAMP),
        NULL
    UNION ALL
    -- Error case: NULL required field (prescriber_key)
    SELECT
        'SHIPERR09',
        'PRSCERR09',
        NULL,
        'PATERR09',
        'CLOSE',
        'CASEERR09',
        'ACCERR09',
        sha2(concat('SHIPERR09', '|', 'PRSCERR09', '|', 'PATERR09', '|', 'ACCERR09'), 256),
        CAST('2024-06-14T19:00:00.000+0000' AS TIMESTAMP),
        CAST('2024-06-27' AS DATE)
    UNION ALL
    -- Error case: NULL required field (patient_sf_id)
    SELECT
        'SHIPERR10',
        'PRSCERR10',
        'KEY015',
        NULL,
        'NEW',
        'CASEERR10',
        'ACCERR10',
        sha2(concat('SHIPERR10', '|', 'PRSCERR10', '|', '', '|', 'ACCERR10'), 256),
        CAST('2024-06-15T20:00:00.000+0000' AS TIMESTAMP),
        NULL
    UNION ALL
    -- Error case: NULL required field (service_request_type)
    SELECT
        'SHIPERR11',
        'PRSCERR11',
        'KEY016',
        'PATERR11',
        NULL,
        'CASEERR11',
        'ACCERR11',
        sha2(concat('SHIPERR11', '|', 'PRSCERR11', '|', 'PATERR11', '|', 'ACCERR11'), 256),
        CAST('2024-06-16T21:00:00.000+0000' AS TIMESTAMP),
        CAST('2024-06-28' AS DATE)
    UNION ALL
    -- Error case: NULL required field (case_sf_id)
    SELECT
        'SHIPERR12',
        'PRSCERR12',
        'KEY017',
        'PATERR12',
        'UPDATE',
        NULL,
        'ACCERR12',
        sha2(concat('SHIPERR12', '|', 'PRSCERR12', '|', 'PATERR12', '|', 'ACCERR12'), 256),
        CAST('2024-06-17T22:00:00.000+0000' AS TIMESTAMP),
        NULL
    UNION ALL
    -- Error case: NULL required field (account_id)
    SELECT
        'SHIPERR13',
        'PRSCERR13',
        'KEY018',
        'PATERR13',
        'CLOSE',
        'CASEERR13',
        NULL,
        sha2(concat('SHIPERR13', '|', 'PRSCERR13', '|', 'PATERR13', '|', ''), 256),
        CAST('2024-06-18T23:00:00.000+0000' AS TIMESTAMP),
        CAST('2024-06-29' AS DATE)
    UNION ALL
    -- Error case: NULL required field (hash_key)
    SELECT
        'SHIPERR14',
        'PRSCERR14',
        'KEY019',
        'PATERR14',
        'NEW',
        'CASEERR14',
        'ACCERR14',
        NULL,
        CAST('2024-06-19T00:00:00.000+0000' AS TIMESTAMP),
        NULL
    UNION ALL
    -- Error case: NULL required field (last_modified_date)
    SELECT
        'SHIPERR15',
        'PRSCERR15',
        'KEY020',
        'PATERR15',
        'UPDATE',
        'CASEERR15',
        'ACCERR15',
        sha2(concat('SHIPERR15', '|', 'PRSCERR15', '|', 'PATERR15', '|', 'ACCERR15'), 256),
        NULL,
        CAST('2024-06-30' AS DATE)
    UNION ALL
    -- Special characters and multi-byte characters
    SELECT
        'SHIP特殊字符',
        'PRSC多字节',
        'KEY021',
        'PAT特殊字符',
        'NEW',
        'CASE特殊字符',
        'ACC特殊字符',
        sha2(concat('SHIP特殊字符', '|', 'PRSC多字节', '|', 'PAT特殊字符', '|', 'ACC特殊字符'), 256),
        CAST('2024-06-20T01:23:45.000+0000' AS TIMESTAMP),
        CAST('2024-07-01' AS DATE)
    UNION ALL
    -- Edge case: planned_date is far in the past
    SELECT
        'SHIPPAST',
        'PRSCPAST',
        'KEY022',
        'PATPAST01',
        'UPDATE',
        'CASEPAST01',
        'ACCPAST01',
        sha2(concat('SHIPPAST', '|', 'PRSCPAST', '|', 'PATPAST01', '|', 'ACCPAST01'), 256),
        CAST('2024-06-21T02:34:56.000+0000' AS TIMESTAMP),
        CAST('1900-01-01' AS DATE)
    UNION ALL
    -- Edge case: planned_date is far in the future
    SELECT
        'SHIPFUTURE',
        'PRSCFUTURE',
        'KEY023',
        'PATFUTURE01',
        'CLOSE',
        'CASEFUTURE01',
        'ACCFUTURE01',
        sha2(concat('SHIPFUTURE', '|', 'PRSCFUTURE', '|', 'PATFUTURE01', '|', 'ACCFUTURE01'), 256),
        CAST('2024-06-22T03:45:00.000+0000' AS TIMESTAMP),
        CAST('2099-12-31' AS DATE)
    UNION ALL
    -- Error case: duplicate prescriber_key for prescriber_id
    SELECT
        'SHIP123456',
        'PRSC7890',
        'KEY001',
        'PATDUPLICATE',
        'NEW',
        'CASEDUPLICATE',
        'ACCDUPLICATE',
        sha2(concat('SHIP123456', '|', 'PRSC7890', '|', 'PATDUPLICATE', '|', 'ACCDUPLICATE'), 256),
        CAST('2024-06-23T04:56:00.000+0000' AS TIMESTAMP),
        CAST('2024-07-02' AS DATE)
)
-- Insert the test data into the patient_account table
INSERT INTO purgo_databricks.purgo_playground.patient_account
SELECT *
FROM test_patient_account_data;
