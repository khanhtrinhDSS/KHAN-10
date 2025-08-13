USE CATALOG purgo_databricks;

/*
================================================================================
Inventory at Risk Calculation for f_inv_movmnt Table
================================================================================
Catalog: purgo_databricks
Schema: purgo_playground
Table: f_inv_movmnt

Business Logic:
- Inventory at risk is defined as records where:
    * flag_active = 'Y'
    * financial_qty > 0
    * expired_qt = 0
- Grouping: By item_nbr
- Output: item_nbr, total_financial_qty (sum of financial_qty, rounded to 2 decimals)
- Only the latest updt_dt per item_nbr is considered
- Data quality checks: Exclude records with NULLs in required fields

================================================================================
Column Comments for Documentation
================================================================================
*/
COMMENT ON COLUMN purgo_playground.f_inv_movmnt.flag_active IS
  'Active flag. Valid values: "Y" (active), "N" (inactive). Used to filter active inventory at risk.';
COMMENT ON COLUMN purgo_playground.f_inv_movmnt.financial_qty IS
  'Financial quantity. Used for inventory at risk calculation. Must be positive DOUBLE for at risk.';
COMMENT ON COLUMN purgo_playground.f_inv_movmnt.expired_qt IS
  'Expired quantity. Inventory at risk must have expired_qt = 0.';
COMMENT ON COLUMN purgo_playground.f_inv_movmnt.item_nbr IS
  'Item number. Grouping dimension for inventory at risk summary.';
COMMENT ON COLUMN purgo_playground.f_inv_movmnt.updt_dt IS
  'Last update timestamp. Used to select latest record per item_nbr for inventory at risk calculation.';

/*
================================================================================
CTE: Latest At Risk Inventory Records by item_nbr
================================================================================
*/
WITH latest_at_risk AS (
  SELECT
    item_nbr,
    MAX(updt_dt) AS latest_updt_dt
  FROM purgo_playground.f_inv_movmnt
  WHERE flag_active = 'Y'
    AND financial_qty > 0
    AND expired_qt = 0
    AND item_nbr IS NOT NULL
    AND financial_qty IS NOT NULL
    AND expired_qt IS NOT NULL
    AND updt_dt IS NOT NULL
  GROUP BY item_nbr
)

/*
================================================================================
Inventory at Risk Summary Query
================================================================================
- Returns: item_nbr, total_financial_qty
- Only includes records matching business logic and latest updt_dt per item_nbr
- Output is ordered by item_nbr
================================================================================
*/
SELECT
  m.item_nbr AS item_nbr,
  ROUND(SUM(m.financial_qty), 2) AS total_financial_qty
FROM purgo_playground.f_inv_movmnt m
INNER JOIN latest_at_risk l
  ON m.item_nbr = l.item_nbr AND m.updt_dt = l.latest_updt_dt
WHERE m.flag_active = 'Y'
  AND m.financial_qty > 0
  AND m.expired_qt = 0
  AND m.item_nbr IS NOT NULL
  AND m.financial_qty IS NOT NULL
  AND m.expired_qt IS NOT NULL
  AND m.updt_dt IS NOT NULL
GROUP BY m.item_nbr
ORDER BY m.item_nbr
;
-- End of Inventory at Risk Calculation SQL
