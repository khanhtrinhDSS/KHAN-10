USE CATALOG purgo_databricks;

-- Test Data Generation for purgo_playground.f_inv_movmnt
-- Covers: Happy path, edge cases, error cases, NULLs, special/multibyte characters

CREATE OR REPLACE TABLE purgo_playground.f_inv_movmnt (
  txn_id STRING,
  inv_loc STRING,
  financial_qty DOUBLE,
  net_qty DOUBLE,
  expired_qt DECIMAL(38,0),
  item_nbr STRING,
  unit_cost DOUBLE,
  uom_rate DOUBLE,
  plant_loc_cd STRING,
  inv_stock_reference STRING,
  stock_type STRING,
  qty_on_hand DOUBLE,
  qty_shipped DOUBLE,
  cancel_dt DECIMAL(38,0),
  flag_active STRING,
  crt_dt TIMESTAMP,
  updt_dt TIMESTAMP
);

WITH test_data AS (
  SELECT
    -- Happy path: valid, active, at risk
    'TXN001' AS txn_id, 'LOC01' AS inv_loc, 100.50 AS financial_qty, 95.00 AS net_qty, 0 AS expired_qt, 'ITEM001' AS item_nbr,
    10.00 AS unit_cost, 1.0 AS uom_rate, 'PLANT01' AS plant_loc_cd, 'REF001' AS inv_stock_reference, 'A' AS stock_type,
    120.00 AS qty_on_hand, 20.00 AS qty_shipped, NULL AS cancel_dt, 'Y' AS flag_active,
    CAST('2024-03-21T08:00:00.000+0000' AS TIMESTAMP) AS crt_dt, CAST('2024-03-21T09:00:00.000+0000' AS TIMESTAMP) AS updt_dt
  UNION ALL
    -- Happy path: valid, active, at risk, different item/location
    SELECT 'TXN002', 'LOC02', 200.00, 180.00, 0, 'ITEM002', 15.00, 1.2, 'PLANT02', 'REF002', 'B', 210.00, 30.00, NULL, 'Y',
      CAST('2024-03-22T10:00:00.000+0000' AS TIMESTAMP), CAST('2024-03-22T11:00:00.000+0000' AS TIMESTAMP)
  UNION ALL
    -- Edge case: financial_qty just above zero
    SELECT 'TXN003', 'LOC03', 0.01, 0.01, 0, 'ITEM003', 5.00, 0.8, 'PLANT03', 'REF003', 'C', 0.01, 0.00, NULL, 'Y',
      CAST('2024-03-23T12:00:00.000+0000' AS TIMESTAMP), CAST('2024-03-23T13:00:00.000+0000' AS TIMESTAMP)
  UNION ALL
    -- Edge case: expired_qt at boundary (not at risk)
    SELECT 'TXN004', 'LOC04', 50.00, 45.00, 1, 'ITEM004', 8.00, 1.1, 'PLANT04', 'REF004', 'D', 60.00, 10.00, NULL, 'Y',
      CAST('2024-03-24T14:00:00.000+0000' AS TIMESTAMP), CAST('2024-03-24T15:00:00.000+0000' AS TIMESTAMP)
  UNION ALL
    -- Error case: financial_qty negative
    SELECT 'TXN005', 'LOC05', -10.00, -10.00, 0, 'ITEM005', 12.00, 1.3, 'PLANT05', 'REF005', 'E', -10.00, 0.00, NULL, 'Y',
      CAST('2024-03-25T16:00:00.000+0000' AS TIMESTAMP), CAST('2024-03-25T17:00:00.000+0000' AS TIMESTAMP)
  UNION ALL
    -- Error case: expired_qt negative
    SELECT 'TXN006', 'LOC06', 30.00, 30.00, -5, 'ITEM006', 7.00, 0.9, 'PLANT06', 'REF006', 'F', 35.00, 5.00, NULL, 'Y',
      CAST('2024-03-26T18:00:00.000+0000' AS TIMESTAMP), CAST('2024-03-26T19:00:00.000+0000' AS TIMESTAMP)
  UNION ALL
    -- Error case: flag_active not 'Y'
    SELECT 'TXN007', 'LOC07', 80.00, 75.00, 0, 'ITEM007', 9.00, 1.0, 'PLANT07', 'REF007', 'G', 90.00, 10.00, NULL, 'N',
      CAST('2024-03-27T20:00:00.000+0000' AS TIMESTAMP), CAST('2024-03-27T21:00:00.000+0000' AS TIMESTAMP)
  UNION ALL
    -- Error case: flag_active NULL
    SELECT 'TXN008', 'LOC08', 60.00, 55.00, 0, 'ITEM008', 11.00, 1.2, 'PLANT08', 'REF008', 'H', 65.00, 5.00, NULL, NULL,
      CAST('2024-03-28T22:00:00.000+0000' AS TIMESTAMP), CAST('2024-03-28T23:00:00.000+0000' AS TIMESTAMP)
  UNION ALL
    -- Error case: financial_qty NULL
    SELECT 'TXN009', 'LOC09', NULL, 40.00, 0, 'ITEM009', 13.00, 1.1, 'PLANT09', 'REF009', 'I', 45.00, 5.00, NULL, 'Y',
      CAST('2024-03-29T00:00:00.000+0000' AS TIMESTAMP), CAST('2024-03-29T01:00:00.000+0000' AS TIMESTAMP)
  UNION ALL
    -- Error case: expired_qt NULL
    SELECT 'TXN010', 'LOC10', 70.00, 65.00, NULL, 'ITEM010', 14.00, 1.0, 'PLANT10', 'REF010', 'J', 75.00, 5.00, NULL, 'Y',
      CAST('2024-03-30T02:00:00.000+0000' AS TIMESTAMP), CAST('2024-03-30T03:00:00.000+0000' AS TIMESTAMP)
  UNION ALL
    -- Error case: financial_qty as string (invalid type, for error scenario)
    SELECT 'TXN011', 'LOC11', CAST('abc' AS DOUBLE), 50.00, 0, 'ITEM011', 15.00, 1.2, 'PLANT11', 'REF011', 'K', 55.00, 5.00, NULL, 'Y',
      CAST('2024-03-31T04:00:00.000+0000' AS TIMESTAMP), CAST('2024-03-31T05:00:00.000+0000' AS TIMESTAMP)
  UNION ALL
    -- Error case: flag_active as 'y' (lowercase)
    SELECT 'TXN012', 'LOC12', 90.00, 85.00, 0, 'ITEM012', 16.00, 1.3, 'PLANT12', 'REF012', 'L', 95.00, 5.00, NULL, 'y',
      CAST('2024-04-01T06:00:00.000+0000' AS TIMESTAMP), CAST('2024-04-01T07:00:00.000+0000' AS TIMESTAMP)
  UNION ALL
    -- Error case: flag_active as 1 (invalid type)
    SELECT 'TXN013', 'LOC13', 100.00, 95.00, 0, 'ITEM013', 17.00, 1.4, 'PLANT13', 'REF013', 'M', 105.00, 5.00, NULL, '1',
      CAST('2024-04-02T08:00:00.000+0000' AS TIMESTAMP), CAST('2024-04-02T09:00:00.000+0000' AS TIMESTAMP)
  UNION ALL
    -- NULL handling: all nullable columns NULL
    SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
      NULL, NULL
  UNION ALL
    -- Special characters: item_nbr with emoji, inv_loc with unicode
    SELECT 'TXN014', 'LÖC14', 110.00, 105.00, 0, 'ITEM💊014', 18.00, 1.5, 'PLANT14', 'REF014', 'N', 115.00, 5.00, NULL, 'Y',
      CAST('2024-04-03T10:00:00.000+0000' AS TIMESTAMP), CAST('2024-04-03T11:00:00.000+0000' AS TIMESTAMP)
  UNION ALL
    -- Special characters: inv_stock_reference with special chars
    SELECT 'TXN015', 'LOC15', 120.00, 115.00, 0, 'ITEM015', 19.00, 1.6, 'PLANT15', 'REF!@#015', 'O', 125.00, 5.00, NULL, 'Y',
      CAST('2024-04-04T12:00:00.000+0000' AS TIMESTAMP), CAST('2024-04-04T13:00:00.000+0000' AS TIMESTAMP)
  UNION ALL
    -- Edge case: financial_qty very large
    SELECT 'TXN016', 'LOC16', 9999999999.99, 9999999999.99, 0, 'ITEM016', 20.00, 1.7, 'PLANT16', 'REF016', 'P', 9999999999.99, 0.00, NULL, 'Y',
      CAST('2024-04-05T14:00:00.000+0000' AS TIMESTAMP), CAST('2024-04-05T15:00:00.000+0000' AS TIMESTAMP)
  UNION ALL
    -- Edge case: financial_qty zero (not at risk)
    SELECT 'TXN017', 'LOC17', 0.00, 0.00, 0, 'ITEM017', 21.00, 1.8, 'PLANT17', 'REF017', 'Q', 0.00, 0.00, NULL, 'Y',
      CAST('2024-04-06T16:00:00.000+0000' AS TIMESTAMP), CAST('2024-04-06T17:00:00.000+0000' AS TIMESTAMP)
  UNION ALL
    -- Edge case: expired_qt very large (not at risk)
    SELECT 'TXN018', 'LOC18', 130.00, 125.00, 9999999999, 'ITEM018', 22.00, 1.9, 'PLANT18', 'REF018', 'R', 135.00, 5.00, NULL, 'Y',
      CAST('2024-04-07T18:00:00.000+0000' AS TIMESTAMP), CAST('2024-04-07T19:00:00.000+0000' AS TIMESTAMP)
  UNION ALL
    -- Edge case: cancel_dt set
    SELECT 'TXN019', 'LOC19', 140.00, 135.00, 0, 'ITEM019', 23.00, 2.0, 'PLANT19', 'REF019', 'S', 145.00, 5.00, 20240408, 'Y',
      CAST('2024-04-08T20:00:00.000+0000' AS TIMESTAMP), CAST('2024-04-08T21:00:00.000+0000' AS TIMESTAMP)
  UNION ALL
    -- Edge case: updt_dt in the future
    SELECT 'TXN020', 'LOC20', 150.00, 145.00, 0, 'ITEM020', 24.00, 2.1, 'PLANT20', 'REF020', 'T', 155.00, 5.00, NULL, 'Y',
      CAST('2024-04-09T22:00:00.000+0000' AS TIMESTAMP), CAST('2025-01-01T00:00:00.000+0000' AS TIMESTAMP)
  UNION ALL
    -- Edge case: updt_dt in the past
    SELECT 'TXN021', 'LOC21', 160.00, 155.00, 0, 'ITEM021', 25.00, 2.2, 'PLANT21', 'REF021', 'U', 165.00, 5.00, NULL, 'Y',
      CAST('2023-01-01T00:00:00.000+0000' AS TIMESTAMP), CAST('2023-01-02T00:00:00.000+0000' AS TIMESTAMP)
  UNION ALL
    -- Special/multibyte: item_nbr with Chinese characters
    SELECT 'TXN022', 'LOC22', 170.00, 165.00, 0, '药品022', 26.00, 2.3, 'PLANT22', 'REF022', 'V', 175.00, 5.00, NULL, 'Y',
      CAST('2024-04-10T00:00:00.000+0000' AS TIMESTAMP), CAST('2024-04-10T01:00:00.000+0000' AS TIMESTAMP)
  UNION ALL
    -- Special/multibyte: inv_loc with Japanese characters
    SELECT 'TXN023', '倉庫23', 180.00, 175.00, 0, 'ITEM023', 27.00, 2.4, 'PLANT23', 'REF023', 'W', 185.00, 5.00, NULL, 'Y',
      CAST('2024-04-11T02:00:00.000+0000' AS TIMESTAMP), CAST('2024-04-11T03:00:00.000+0000' AS TIMESTAMP)
  UNION ALL
    -- Special/multibyte: plant_loc_cd with Korean characters
    SELECT 'TXN024', 'LOC24', 190.00, 185.00, 0, 'ITEM024', 28.00, 2.5, '공장24', 'REF024', 'X', 195.00, 5.00, NULL, 'Y',
      CAST('2024-04-12T04:00:00.000+0000' AS TIMESTAMP), CAST('2024-04-12T05:00:00.000+0000' AS TIMESTAMP)
  UNION ALL
    -- Edge case: all string fields empty
    SELECT '', '', 200.00, 195.00, 0, '', 29.00, 2.6, '', '', '', 205.00, 5.00, NULL, 'Y',
      CAST('2024-04-13T06:00:00.000+0000' AS TIMESTAMP), CAST('2024-04-13T07:00:00.000+0000' AS TIMESTAMP)
  UNION ALL
    -- Edge case: all string fields with only spaces
    SELECT ' ', ' ', 210.00, 205.00, 0, ' ', 30.00, 2.7, ' ', ' ', ' ', 215.00, 5.00, NULL, 'Y',
      CAST('2024-04-14T08:00:00.000+0000' AS TIMESTAMP), CAST('2024-04-14T09:00:00.000+0000' AS TIMESTAMP)
)
INSERT INTO purgo_playground.f_inv_movmnt
SELECT * FROM test_data;
