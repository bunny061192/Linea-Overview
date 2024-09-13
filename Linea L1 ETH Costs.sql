WITH data_submission_costs AS (
  SELECT
    DATE(ds.evt_block_time) AS day,
    TRY_CAST(SUM(tx.gas_used) AS DECIMAL(38, 0)) * TRY_CAST(AVG(tx.gas_price) AS DECIMAL(38, 0)) / 1e18 AS data_submission_cost_eth
  FROM linea_v2_ethereum.LineaRollup_evt_DataSubmitted AS ds
  JOIN ethereum.transactions AS tx ON ds.evt_tx_hash = tx.hash
  WHERE ds.evt_block_time >= CURRENT_DATE - INTERVAL '90' day
  GROUP BY DATE(ds.evt_block_time)
), blob_costs AS (
  SELECT
    DATE(log.block_time) AS day,
    TRY_CAST(SUM(log.blob_gas_used) AS DECIMAL(38, 0)) * TRY_CAST(AVG(log.blob_gas_price) AS DECIMAL(38, 0)) / 1e18 AS blob_cost_eth
  FROM ethereum.logs AS log
  JOIN ethereum.transactions AS tx ON log.tx_hash = tx.hash
  JOIN linea_v2_ethereum.LineaRollup_evt_DataSubmitted AS ds ON ds.evt_tx_hash = tx.hash
  WHERE log.block_time >= CURRENT_DATE - INTERVAL '90' day
  GROUP BY DATE(log.block_time)
), finalization_costs AS (
  SELECT
    DATE(t.block_time) AS day,
    SUM(
      TRY_CAST(t.gas_used AS DECIMAL(38, 0)) * TRY_CAST(t.gas_price AS DECIMAL(38, 0)) / 1e18
    ) AS finalization_cost_eth
  FROM ethereum.transactions t
  WHERE t.block_time >= CURRENT_DATE - INTERVAL '90' day
    AND t.to = 0xd19d4B5d358258f05D7B411E21A1460D11B0876F
    AND bytearray_substring(t.data, 1, 4) = 0xd630280f
  GROUP BY DATE(t.block_time)
)
SELECT
  COALESCE(dsc.day, bc.day, fc.day) AS day,
  COALESCE(dsc.data_submission_cost_eth, 0) AS data_submission_cost_eth,
  COALESCE(bc.blob_cost_eth, 0) AS blob_cost_eth,
  COALESCE(fc.finalization_cost_eth, 0) AS finalization_cost_eth,
  (COALESCE(dsc.data_submission_cost_eth, 0) + COALESCE(bc.blob_cost_eth, 0) + COALESCE(fc.finalization_cost_eth, 0)) AS total_eth_spent
FROM data_submission_costs dsc
FULL OUTER JOIN blob_costs bc ON dsc.day = bc.day
FULL OUTER JOIN finalization_costs fc ON dsc.day = fc.day OR bc.day = fc.day
ORDER BY day DESC;
