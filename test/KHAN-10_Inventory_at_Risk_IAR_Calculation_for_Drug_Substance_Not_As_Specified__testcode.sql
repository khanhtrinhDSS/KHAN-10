/* 
================================================================================
Databricks SQL Test Suite: Inventory at Risk Calculation for f_inv_movmnt Table
================================================================================

Catalog: purgo_databricks
Schema: purgo_playground
Table: f_inv_movmnt

Test Coverage:
- Schema validation
- Data type validation
- Business logic validation
- NULL and error handling
- Output format validation
- Data quality checks
- Edge cases and special characters
- Delta Lake operations
- Window function analytics
- Cleanup operations

================================================================================
Setup: Ensure Catalog and Schema Context
================================================================================
*/
USE CATALOG purgo_databricks;
USE purgo_playground;

/* 
================================================================================
Section: Schema Validation for f_inv_movmnt
================================================================================
-- Validate that the table exists and has the expected columns and data types
*/
-- Assert table existence
SELECT
  CASE WHEN COUNT(*) = 1 THEN 1 ELSE RAISE_ERROR("Table purgo_playground.f_inv_movmnt does not exist") END AS table_exists
FROM information_schema.tables
WHERE table_catalog = "purgo_databricks"
  AND table_schema = "purgo_playground"
  AND table_name = "f_inv_movmnt";

-- Assert column existence and data types
WITH expected_schema AS (
  SELECT "txn_id" AS column_name, "STRING" AS data_type UNION ALL
  SELECT "inv_loc", "STRING" UNION ALL
  SELECT "financial_qty", "DOUBLE" UNION ALL
  SELECT "net_qty", "DOUBLE" UNION ALL
  SELECT "expired_qt", "DECIMAL(38,0)" UNION ALL
  SELECT "item_nbr", "STRING" UNION ALL
  SELECT "unit_cost", "DOUBLE" UNION ALL
  SELECT "uom_rate", "DOUBLE" UNION ALL
  SELECT "plant_loc_cd", "STRING" UNION ALL
  SELECT "inv_stock_reference", "STRING" UNION ALL
  SELECT "stock_type", "STRING" UNION ALL
  SELECT "qty_on_hand", "DOUBLE" UNION ALL
  SELECT "qty_shipped", "DOUBLE" UNION ALL
  SELECT "cancel_dt", "DECIMAL(38,0)" UNION ALL
  SELECT "flag_active", "STRING" UNION ALL
  SELECT "crt_dt", "TIMESTAMP" UNION ALL
  SELECT "updt_dt", "TIMESTAMP"
)
SELECT
  CASE WHEN COUNT(*) = 0 THEN 1 ELSE RAISE_ERROR("Schema mismatch in purgo_playground.f_inv_movmnt") END AS schema_valid
FROM (
  SELECT e.column_name, e.data_type
  FROM expected_schema e
  LEFT JOIN information_schema.columns c
    ON c.table_catalog = "purgo_databricks"
    AND c.table_schema = "purgo_playground"
    AND c.table_name = "f_inv_movmnt"
    AND c.column_name = e.column_name
    AND c.data_type = e.data_type
  WHERE c.column_name IS NULL
);

/* 
================================================================================
Section: Data Type Conversion and NULL Handling Tests
================================================================================
-- Validate that financial_qty is DOUBLE and not NULL for at risk records
-- Validate that expired_qt is DECIMAL(38,0) and not NULL for at risk records
-- Validate that flag_active is STRING and only 'Y' is allowed for active records
*/
-- Assert only allowed flag_active values
SELECT
  CASE WHEN COUNT(*) = 0 THEN 1 ELSE RAISE_ERROR("Invalid flag_active values found in purgo_playground.f_inv_movmnt") END AS flag_active_valid
FROM purgo_playground.f_inv_movmnt
WHERE flag_active IS NOT NULL AND flag_active NOT IN ("Y", "N");

-- Assert financial_qty is DOUBLE and not NULL for at risk records
SELECT
  CASE WHEN COUNT(*) = 0 THEN 1 ELSE RAISE_ERROR("Invalid financial_qty values for at risk records") END AS financial_qty_valid
FROM purgo_playground.f_inv_movmnt
WHERE flag_active = "Y" AND (financial_qty IS NULL OR financial_qty <= 0);

-- Assert expired_qt is DECIMAL(38,0) and not NULL for at risk records
SELECT
  CASE WHEN COUNT(*) = 0 THEN 1 ELSE RAISE_ERROR("Invalid expired_qt values for at risk records") END AS expired_qt_valid
FROM purgo_playground.f_inv_movmnt
WHERE flag_active = "Y" AND (expired_qt IS NULL OR expired_qt <> 0);

/* 
================================================================================
Section: Business Logic Validation for Inventory at Risk
================================================================================
-- Definition: Inventory at risk = flag_active = 'Y' AND financial_qty > 0 AND expired_qt = 0
-- Grouping: By item_nbr
-- Output: item_nbr, total_financial_qty (sum of financial_qty)
-- Date: As of latest updt_dt per item_nbr
*/
-- CTE: Select latest updt_dt per item_nbr for at risk records
WITH latest_at_risk AS (
  SELECT
    item_nbr,
    MAX(updt_dt) AS latest_updt_dt
  FROM purgo_playground.f_inv_movmnt
  WHERE flag_active = "Y"
    AND financial_qty > 0
    AND expired_qt = 0
    AND item_nbr IS NOT NULL
  GROUP BY item_nbr
)
-- Validation Query: Calculate inventory at risk summary
SELECT
  m.item_nbr,
  ROUND(SUM(m.financial_qty), 2) AS total_financial_qty
FROM purgo_playground.f_inv_movmnt m
INNER JOIN latest_at_risk l
  ON m.item_nbr = l.item_nbr AND m.updt_dt = l.latest_updt_dt
WHERE m.flag_active = "Y"
  AND m.financial_qty > 0
  AND m.expired_qt = 0
  AND m.item_nbr IS NOT NULL
GROUP BY m.item_nbr
ORDER BY m.item_nbr;

/* 
================================================================================
Section: Output Format and Data Type Validation
================================================================================
-- Assert output columns and types
*/
-- Assert output columns: item_nbr (STRING), total_financial_qty (DOUBLE)
SELECT
  CASE
    WHEN typeof(item_nbr) = "STRING" AND typeof(total_financial_qty) = "DOUBLE" THEN 1
    ELSE RAISE_ERROR("Output column types mismatch: item_nbr or total_financial_qty")
  END AS output_types_valid
FROM (
  SELECT
    m.item_nbr,
    ROUND(SUM(m.financial_qty), 2) AS total_financial_qty
  FROM purgo_playground.f_inv_movmnt m
  INNER JOIN (
    SELECT item_nbr, MAX(updt_dt) AS latest_updt_dt
    FROM purgo_playground.f_inv_movmnt
    WHERE flag_active = "Y" AND financial_qty > 0 AND expired_qt = 0 AND item_nbr IS NOT NULL
    GROUP BY item_nbr
  ) l
    ON m.item_nbr = l.item_nbr AND m.updt_dt = l.latest_updt_dt
  WHERE m.flag_active = "Y" AND m.financial_qty > 0 AND m.expired_qt = 0 AND m.item_nbr IS NOT NULL
  GROUP BY m.item_nbr
  LIMIT 1
);

/* 
================================================================================
Section: Data Quality Validation
================================================================================
-- Assert no duplicate item_nbr in output
*/
WITH summary AS (
  SELECT
    m.item_nbr,
    ROUND(SUM(m.financial_qty), 2) AS total_financial_qty
  FROM purgo_playground.f_inv_movmnt m
  INNER JOIN (
    SELECT item_nbr, MAX(updt_dt) AS latest_updt_dt
    FROM purgo_playground.f_inv_movmnt
    WHERE flag_active = "Y" AND financial_qty > 0 AND expired_qt = 0 AND item_nbr IS NOT NULL
    GROUP BY item_nbr
  ) l
    ON m.item_nbr = l.item_nbr AND m.updt_dt = l.latest_updt_dt
  WHERE m.flag_active = "Y" AND m.financial_qty > 0 AND m.expired_qt = 0 AND m.item_nbr IS NOT NULL
  GROUP BY m.item_nbr
)
SELECT
  CASE WHEN COUNT(*) = COUNT(DISTINCT item_nbr) THEN 1 ELSE RAISE_ERROR("Duplicate item_nbr found in output") END AS no_duplicates
FROM summary;

/* 
================================================================================
Section: Error Handling Tests
================================================================================
-- Assert error for invalid flag_active values
*/
SELECT
  CASE WHEN COUNT(*) = 0 THEN 1 ELSE RAISE_ERROR("Invalid flag_active value(s) found. Expected 'Y'.") END AS invalid_flag_active
FROM purgo_playground.f_inv_movmnt
WHERE flag_active IS NOT NULL AND flag_active NOT IN ("Y", "N");

/* 
-- Assert error for invalid financial_qty values
*/
SELECT
  CASE WHEN COUNT(*) = 0 THEN 1 ELSE RAISE_ERROR("Invalid financial_qty value(s) found. Must be positive double.") END AS invalid_financial_qty
FROM purgo_playground.f_inv_movmnt
WHERE flag_active = "Y" AND (financial_qty IS NULL OR financial_qty <= 0);

/* 
-- Assert error for invalid expired_qt values
*/
SELECT
  CASE WHEN COUNT(*) = 0 THEN 1 ELSE RAISE_ERROR("Invalid expired_qt value(s) found. Must be 0 for at risk inventory.") END AS invalid_expired_qt
FROM purgo_playground.f_inv_movmnt
WHERE flag_active = "Y" AND (expired_qt IS NULL OR expired_qt <> 0);

/* 
================================================================================
Section: Window Function Analytics Test
================================================================================
-- Example: Calculate running total of financial_qty for at risk inventory by item_nbr
*/
WITH at_risk AS (
  SELECT
    txn_id,
    item_nbr,
    financial_qty,
    updt_dt
  FROM purgo_playground.f_inv_movmnt
  WHERE flag_active = "Y" AND financial_qty > 0 AND expired_qt = 0 AND item_nbr IS NOT NULL
)
SELECT
  item_nbr,
  txn_id,
  financial_qty,
  SUM(financial_qty) OVER (PARTITION BY item_nbr ORDER BY updt_dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total_financial_qty
FROM at_risk
ORDER BY item_nbr, updt_dt;

/* 
================================================================================
Section: Delta Lake Operations Test
================================================================================
-- Test MERGE, UPDATE, DELETE on f_inv_movmnt
*/
-- MERGE: Upsert a test record (simulate update of financial_qty for an existing txn_id)
MERGE INTO purgo_playground.f_inv_movmnt AS target
USING (
  SELECT "TXN001" AS txn_id, "LOC01" AS inv_loc, 999.99 AS financial_qty, 95.00 AS net_qty, 0 AS expired_qt, "ITEM001" AS item_nbr,
         10.00 AS unit_cost, 1.0 AS uom_rate, "PLANT01" AS plant_loc_cd, "REF001" AS inv_stock_reference, "A" AS stock_type,
         120.00 AS qty_on_hand, 20.00 AS qty_shipped, NULL AS cancel_dt, "Y" AS flag_active,
         CAST("2024-03-21T08:00:00.000+0000" AS TIMESTAMP) AS crt_dt, CAST("2024-03-21T09:00:00.000+0000" AS TIMESTAMP) AS updt_dt
) AS source
ON target.txn_id = source.txn_id
WHEN MATCHED THEN
  UPDATE SET target.financial_qty = source.financial_qty
WHEN NOT MATCHED THEN
  INSERT (
    txn_id, inv_loc, financial_qty, net_qty, expired_qt, item_nbr, unit_cost, uom_rate, plant_loc_cd, inv_stock_reference,
    stock_type, qty_on_hand, qty_shipped, cancel_dt, flag_active, crt_dt, updt_dt
  ) VALUES (
    source.txn_id, source.inv_loc, source.financial_qty, source.net_qty, source.expired_qt, source.item_nbr, source.unit_cost,
    source.uom_rate, source.plant_loc_cd, source.inv_stock_reference, source.stock_type, source.qty_on_hand, source.qty_shipped,
    source.cancel_dt, source.flag_active, source.crt_dt, source.updt_dt
  );

-- UPDATE: Set financial_qty to 0 for a specific txn_id (simulate removal from at risk)
UPDATE purgo_playground.f_inv_movmnt
SET financial_qty = 0
WHERE txn_id = "TXN001";

-- DELETE: Remove a test record
DELETE FROM purgo_playground.f_inv_movmnt
WHERE txn_id = "TXN001";

/* 
================================================================================
Section: Cleanup Operations
================================================================================
-- Remove test records with txn_id like 'TXN0%'
*/
DELETE FROM purgo_playground.f_inv_movmnt
WHERE txn_id LIKE "TXN0%";

/* 
================================================================================
End of Test Suite
================================================================================
*/

