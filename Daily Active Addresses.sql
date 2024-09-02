WITH daily_fees AS (
  SELECT
    DATE(block_time) AS day,
    COUNT(*) AS transactions_per_day,
    COUNT(DISTINCT "from") AS distinct_sending_addresses_per_day,
    SUM(CAST(gas_used AS DECIMAL(38, 0)) * CAST(gas_price AS DECIMAL(38, 0)) / 1e18) AS fees_earned_per_day -- Cast to DECIMAL to prevent overflow
  FROM linea.transactions
  WHERE
    block_time >= CURRENT_DATE - INTERVAL '180' day
  GROUP BY
    DATE(block_time)
), blob_costs AS (
  SELECT
    t.block_time,
    t.block_date AS day,
    SUM(bs.blob_gas_used * bs.blob_base_fee / 1e18) AS blob_cost_per_tx
  FROM ethereum.transactions t
  JOIN ethereum.blobs_submissions bs ON t.hash = bs.tx_hash
  WHERE
    t.block_time >= CURRENT_DATE - INTERVAL '180' day
    AND t.to = 0xd19d4B5d358258f05D7B411E21A1460D11B0876F
    AND (
        bytearray_substring(t.data, 1, 4) = 0xd630280f -- finalizeCompressedBlocksWithProof
        OR
        bytearray_substring(t.data, 1, 4) = 0x7a776315 -- submitData
        OR
        bytearray_substring(t.data, 1, 4) = 0x4165d6dd -- finalizeBlocks
        OR
        bytearray_substring(t.data, 1, 4) = 0x2d3c12e5 -- submitBlobData
        OR
        bytearray_substring(t.data, 1, 4) = 0xabffac32 -- finalizeBlocksWithProof
        OR
        bytearray_substring(t.data, 1, 4) = 0x42fbe842 -- submitBlobs
        )
  GROUP BY
    t.block_time, t.block_date
), daily_costs AS (
  SELECT
    DATE(t.block_time) AS day,
    SUM(CAST(t.gas_used AS DECIMAL(38, 0)) * CAST(t.gas_price AS DECIMAL(38, 0)) / 1e18) + COALESCE(SUM(bc.blob_cost_per_tx), 0) AS costs_per_day -- Include blob costs
  FROM ethereum.transactions t
  LEFT JOIN blob_costs bc ON t.block_time = bc.block_time
  WHERE
    t.block_time >= CURRENT_DATE - INTERVAL '180' day
    AND t.to = 0xd19d4B5d358258f05D7B411E21A1460D11B0876F
    AND (
        bytearray_substring(t.data, 1, 4) = 0xd630280f -- finalizeCompressedBlocksWithProof
        OR
        bytearray_substring(t.data, 1, 4) = 0x7a776315 -- submitData
        OR
        bytearray_substring(t.data, 1, 4) = 0x4165d6dd -- finalizeBlocks
        OR
        bytearray_substring(t.data, 1, 4) = 0x2d3c12e5 -- submitBlobData
        OR
        bytearray_substring(t.data, 1, 4) = 0xabffac32 -- finalizeBlocksWithProof
        OR
        bytearray_substring(t.data, 1, 4) = 0x42fbe842 -- submitBlobs
        )
  GROUP BY
    DATE(t.block_time)
), daily_summary AS (
  SELECT
    df.day,
    df.transactions_per_day,
    df.distinct_sending_addresses_per_day,
    df.fees_earned_per_day,
    -1 * COALESCE(dc.costs_per_day, 0) AS costs_per_day,
    df.fees_earned_per_day - COALESCE(dc.costs_per_day, 0) AS net_revenue_per_day,
    df.transactions_per_day / 86400.0 AS avg_transactions_per_second -- Calculate average TPS
  FROM daily_fees df
  LEFT JOIN daily_costs dc ON df.day = dc.day
)
SELECT
  day,
  transactions_per_day,
  distinct_sending_addresses_per_day,
  fees_earned_per_day,
  costs_per_day,
  net_revenue_per_day,
  avg_transactions_per_second -- Include average TPS in the final selection
FROM daily_summary
ORDER BY
  day DESC;
