USE CATALOG purgo_databricks;

-- Databricks SQL script: ETL logic for trf_inv_txn table using CTEs, field mappings, and lookups
-- Purpose: Populate trf_inv_txn from SAP inventory sources, applying all mapping, transformation, deduplication, and lookup logic
-- Author: Khanh Trinh
-- Date: 2025-10-08
-- Description: This script extracts inventory transaction data from SAP tables (MSEG, MKPF, T001, T001K, MBEW, EKKO, VBAK, AUFK, t156t), applies all field mappings and transformations as per the provided Excel mapping, joins and filters as per Source_Tables, maps movement flags using movment_mapping/lkp_mapping, constructs composite keys, deduplicates using prod_plant_lot_location_key and transaction_id, and outputs the results as a SELECT statement. All logic is implemented using CTEs only, with explicit column aliases and Databricks native types.

-- ========================================================================
-- CTE: movment_mapping_lkp
-- Purpose: Lookup movement type flags for consumption_usage_flag, receipt_flag, scrap_flag, shipment_flag
-- ========================================================================
WITH movment_mapping_lkp AS (
  SELECT
    CAST(`Movement Type` AS STRING) AS movement_type,
    COALESCE(CAST(`Consumption/Usage (Y or N)` AS STRING), "none") AS consumption_usage_flag,
    COALESCE(CAST(`Receipt (Y or N)` AS STRING), "none") AS receipt_flag,
    COALESCE(CAST(`Scrap (Y or N)` AS STRING), "none") AS scrap_flag,
    COALESCE(CAST(`Shipment (Y or N)` AS STRING), "none") AS shipment_flag
  FROM purgo_databricks.purgo_playground.movment_mapping
)

-- ========================================================================
-- CTE: main_inventory_txn
-- Purpose: Extract, join, and transform all required fields for trf_inv_txn from SAP source tables
-- ========================================================================
, main_inventory_txn AS (
  SELECT
    -- company_cd: Fetch BWKEY from T001K by passing Plant from mseg.werks, then pass BWKEY into T001K to fetch company code T001K-BUKRS
    COALESCE(CAST(t001k.bukrs AS STRING), "none") AS company_cd,
    COALESCE(CAST(mseg.matnr AS STRING), "none") AS item_nbr,
    COALESCE(CAST(mseg.lgort AS STRING), "none") AS location_cd,
    COALESCE(CAST(mseg.charg AS STRING), "none") AS lot_nbr,
    COALESCE(CAST(mseg.werks AS STRING), "none") AS plant_cd,
    CAST("mbd" AS STRING) AS src_sys_cd,
    -- transaction_id: concat(mseg.mblnr, mseg.zeile, mseg.mjahr)
    CONCAT_WS("-", mseg.mblnr, mseg.zeile, mseg.mjahr) AS transaction_id,
    COALESCE(CAST(t001.waers AS STRING), "none") AS company_currency_cd,
    COALESCE(CAST(mseg.mblnr AS STRING), "none") AS document_nbr,
    COALESCE(CAST(mseg.sakto AS STRING), "none") AS gl_account_nbr,
    -- posting_dt_yyyymmdd: mkpf.budat in yyyymmdd format
    COALESCE(CAST(mkpf.budat AS STRING), "none") AS posting_dt_yyyymmdd,
    COALESCE(CAST(mseg.meins AS STRING), "none") AS primary_uom_cd,
    COALESCE(CAST(mseg.bukrs AS STRING), "none") AS reference_order_company_cd,
    CAST(1 AS INT) AS reference_order_schedule_line_nbr,
    -- reference_order_type: conditional population based on reference_order_class (not defined, so default to "none")
    CAST("none" AS STRING) AS reference_order_type,
    -- transaction_dt_yyyymmdd: mkpf.bldat in yyyymmdd format
    COALESCE(CAST(mkpf.bldat AS STRING), "none") AS transaction_dt_yyyymmdd,
    -- transaction_qty_primary_uom: IF mseg.shkzg='H' THEN mseg.menge*-1 ELSE mseg.menge
    CASE WHEN mseg.shkzg = 'H' THEN mseg.menge * -1 ELSE mseg.menge END AS transaction_qty_primary_uom,
    -- transaction_qty_transaction_uom: IF mseg.shkzg='H' THEN mseg.erfmg*-1 ELSE mseg.erfmg
    CASE WHEN mseg.shkzg = 'H' THEN mseg.erfmg * -1 ELSE mseg.erfmg END AS transaction_qty_transaction_uom,
    COALESCE(CAST(t156t.btext AS STRING), "none") AS transaction_reason,
    -- transaction_ts: concat(mkpf.cpudt, mkpf.cputm) as 'yyyy-MM-dd HH:mm:ss'
    CASE
      WHEN mkpf.cpudt IS NOT NULL AND mkpf.cputm IS NOT NULL
      THEN
        CAST(
          TO_TIMESTAMP(
            CONCAT(
              SUBSTRING(mkpf.cpudt, 1, 4), "-", SUBSTRING(mkpf.cpudt, 5, 2), "-", SUBSTRING(mkpf.cpudt, 7, 2), " ",
              SUBSTRING(LPAD(mkpf.cputm, 6, '0'), 1, 2), ":", SUBSTRING(LPAD(mkpf.cputm, 6, '0'), 3, 2), ":", SUBSTRING(LPAD(mkpf.cputm, 6, '0'), 5, 2)
            )
          )
          AS TIMESTAMP
        )
      ELSE NULL
    END AS transaction_ts,
    COALESCE(CAST(mkpf.vgart AS STRING), "none") AS transaction_type_cd,
    COALESCE(CAST(mseg.erfme AS STRING), "none") AS transaction_uom,
    -- unit_cost_company_currency: CASE WHEN mbew.vprsv='S' THEN mbew.stprs/mbew.peinh WHEN mbew.vprsv='V' THEN mbew.verpr/mbew.peinh END
    CASE
      WHEN mbew.vprsv = 'S' THEN mbew.stprs / mbew.peinh
      WHEN mbew.vprsv = 'V' THEN mbew.verpr / mbew.peinh
      ELSE NULL
    END AS unit_cost_company_currency,
    -- plant_key: concat_ws('|', src_sys_cd, plant_cd)
    CONCAT_WS('|', "mbd", COALESCE(mseg.werks, "none")) AS plant_key,
    -- prod_key: concat_ws('|', src_sys_cd, item_nbr)
    CONCAT_WS('|', "mbd", COALESCE(mseg.matnr, "none")) AS prod_key,
    -- prod_plant_key: concat_ws('|', src_sys_cd, item_nbr, plant_cd)
    CONCAT_WS('|', "mbd", COALESCE(mseg.matnr, "none"), COALESCE(mseg.werks, "none")) AS prod_plant_key,
    -- plant_location_key: concat_ws('|', src_sys_cd, plant_cd, location_cd)
    CONCAT_WS('|', "mbd", COALESCE(mseg.werks, "none"), COALESCE(mseg.lgort, "none")) AS plant_location_key,
    -- plant_lot_key: concat_ws('|', src_sys_cd, plant_cd, lot_nbr)
    CONCAT_WS('|', "mbd", COALESCE(mseg.werks, "none"), COALESCE(mseg.charg, "none")) AS plant_lot_key,
    -- prod_plant_location_key: concat_ws('|', src_sys_cd, item_nbr, plant_cd, location_cd)
    CONCAT_WS('|', "mbd", COALESCE(mseg.matnr, "none"), COALESCE(mseg.werks, "none"), COALESCE(mseg.lgort, "none")) AS prod_plant_location_key,
    -- prod_plant_lot_key: concat_ws('|', src_sys_cd, item_nbr, plant_cd, lot_nbr)
    CONCAT_WS('|', "mbd", COALESCE(mseg.matnr, "none"), COALESCE(mseg.werks, "none"), COALESCE(mseg.charg, "none")) AS prod_plant_lot_key,
    -- prod_plant_lot_location_key: concat_ws('|', src_sys_cd, item_nbr, plant_cd, lot_nbr, location_cd)
    CONCAT_WS('|', "mbd", COALESCE(mseg.matnr, "none"), COALESCE(mseg.werks, "none"), COALESCE(mseg.charg, "none"), COALESCE(mseg.lgort, "none")) AS prod_plant_lot_location_key,
    -- gl_acct_key: concat_ws('|', src_sys_cd, gl_account_nbr)
    CONCAT_WS('|', "mbd", COALESCE(mseg.sakto, "none")) AS gl_acct_key,
    -- co_key: concat_ws('|', src_sys_cd, company_cd)
    CONCAT_WS('|', "mbd", COALESCE(t001k.bukrs, "none")) AS co_key,
    -- movement flags: join to movment_mapping_lkp by mseg.bwart
    COALESCE(mml.consumption_usage_flag, "none") AS consumption_usage_flag,
    COALESCE(mml.receipt_flag, "none") AS receipt_flag,
    COALESCE(mml.scrap_flag, "none") AS scrap_flag,
    COALESCE(mml.shipment_flag, "none") AS shipment_flag
  FROM purgo_databricks.purgo_playground.mseg mseg
  LEFT OUTER JOIN purgo_databricks.purgo_playground.mkpf mkpf
    ON mkpf.mandt = mseg.mandt
    AND mkpf.mblnr = mseg.mblnr
    AND mkpf.mjahr = mseg.mjahr
    AND CAST(mkpf.budat AS STRING) >= CAST(DATE_FORMAT(DATE_ADD(CURRENT_DATE(), -5 * 365), "yyyyMMdd") AS STRING)
  LEFT OUTER JOIN purgo_databricks.purgo_playground.t001 t001
    ON t001.bukrs = mseg.bukrs
    AND t001.mandt = mseg.mandt
  LEFT OUTER JOIN purgo_databricks.purgo_playground.t001k t001k
    ON t001k.bwkey = mseg.werks
    AND t001k.mandt = mseg.mandt
  LEFT OUTER JOIN purgo_databricks.purgo_playground.mbew mbew
    ON TRIM(mseg.matnr) = TRIM(mbew.matnr)
    AND mseg.werks = mbew.bwkey
    AND TRIM(mbew.bwtar) = ''
    AND mbew.mandt = mseg.mandt
  LEFT OUTER JOIN purgo_databricks.purgo_playground.ekbe ekbe
    ON ekbe.belnr = mseg.mblnr
    AND ekbe.buzei = mseg.zeile
    AND ekbe.gjahr = mseg.mjahr
    AND ekbe.bwart = mseg.bwart
    AND ekbe.mandt = mseg.mandt
  LEFT OUTER JOIN purgo_databricks.purgo_playground.ekko ekko
    ON ekko.ebeln = ekbe.ebeln
    AND TRIM(ekko.ebeln) <> ''
    AND ekko.mandt = mseg.mandt
  LEFT OUTER JOIN purgo_databricks.purgo_playground.t156t t156t
    ON t156t.bwart = mseg.bwart
    AND t156t.sobkz = mseg.sobkz
    AND t156t.kzbew = mseg.kzbew
    AND t156t.kzzug = mseg.kzzug
    AND t156t.kzvbr = mseg.kzvbr
    AND t156t.spras = 'E'
    AND t156t.mandt = mseg.mandt
  LEFT OUTER JOIN movment_mapping_lkp mml
    ON mml.movement_type = mseg.bwart
  WHERE TRIM(mbew.bwtar) = ''
    AND t156t.spras = 'E'
    AND mkpf.budat IS NOT NULL
)

-- ========================================================================
-- CTE: dedup_inventory_txn
-- Purpose: Deduplicate records by composite key (prod_plant_lot_location_key, transaction_id)
-- ========================================================================
, dedup_inventory_txn AS (
  SELECT
    company_cd,
    item_nbr,
    location_cd,
    lot_nbr,
    plant_cd,
    src_sys_cd,
    transaction_id,
    company_currency_cd,
    document_nbr,
    gl_account_nbr,
    posting_dt_yyyymmdd,
    primary_uom_cd,
    reference_order_company_cd,
    reference_order_schedule_line_nbr,
    reference_order_type,
    transaction_dt_yyyymmdd,
    SUM(transaction_qty_primary_uom) AS transaction_qty_primary_uom,
    SUM(transaction_qty_transaction_uom) AS transaction_qty_transaction_uom,
    transaction_reason,
    MAX(transaction_ts) AS transaction_ts,
    transaction_type_cd,
    transaction_uom,
    unit_cost_company_currency,
    plant_key,
    prod_key,
    prod_plant_key,
    plant_location_key,
    plant_lot_key,
    prod_plant_location_key,
    prod_plant_lot_key,
    prod_plant_lot_location_key,
    gl_acct_key,
    co_key,
    consumption_usage_flag,
    receipt_flag,
    scrap_flag,
    shipment_flag
  FROM main_inventory_txn
  GROUP BY
    company_cd,
    item_nbr,
    location_cd,
    lot_nbr,
    plant_cd,
    src_sys_cd,
    transaction_id,
    company_currency_cd,
    document_nbr,
    gl_account_nbr,
    posting_dt_yyyymmdd,
    primary_uom_cd,
    reference_order_company_cd,
    reference_order_schedule_line_nbr,
    reference_order_type,
    transaction_dt_yyyymmdd,
    transaction_reason,
    transaction_type_cd,
    transaction_uom,
    unit_cost_company_currency,
    plant_key,
    prod_key,
    prod_plant_key,
    plant_location_key,
    plant_lot_key,
    prod_plant_location_key,
    prod_plant_lot_key,
    prod_plant_lot_location_key,
    gl_acct_key,
    co_key,
    consumption_usage_flag,
    receipt_flag,
    scrap_flag,
    shipment_flag
)

-- ========================================================================
-- Validation Query: Show deduplicated, transformed trf_inv_txn records
-- ========================================================================
SELECT
  company_cd,
  item_nbr,
  location_cd,
  lot_nbr,
  plant_cd,
  src_sys_cd,
  transaction_id,
  company_currency_cd,
  document_nbr,
  gl_account_nbr,
  posting_dt_yyyymmdd,
  primary_uom_cd,
  reference_order_company_cd,
  reference_order_schedule_line_nbr,
  reference_order_type,
  transaction_dt_yyyymmdd,
  transaction_qty_primary_uom,
  transaction_qty_transaction_uom,
  transaction_reason,
  transaction_ts,
  transaction_type_cd,
  transaction_uom,
  unit_cost_company_currency,
  plant_key,
  prod_key,
  prod_plant_key,
  plant_location_key,
  plant_lot_key,
  prod_plant_location_key,
  prod_plant_lot_key,
  prod_plant_lot_location_key,
  gl_acct_key,
  co_key,
  consumption_usage_flag,
  receipt_flag,
  scrap_flag,
  shipment_flag
FROM dedup_inventory_txn
;
