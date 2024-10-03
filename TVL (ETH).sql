WITH bridge_transfers AS (
  SELECT
    DATE(evt_block_time) AS day,
    SUM(TRY_CAST(_value AS DOUBLE) / 1e18) AS eth_amount
  FROM linea_v2_ethereum.LineaRollup_evt_MessageSent
  GROUP BY
    1
  UNION ALL
  SELECT
    DATE(call_block_time) AS day,
    -SUM(TRY_CAST(_value AS DOUBLE) / 1e18) AS eth_amount
  FROM linea_v2_ethereum.LineaRollup_call_claimMessage
  WHERE
    call_success = TRUE
  GROUP BY
    1
  UNION ALL
  SELECT
    DATE(call_block_time) AS day,
    -SUM(TRY_CAST(JSON_EXTRACT_SCALAR(_params, '$.value') AS DOUBLE) / 1e18) AS eth_amount
  FROM linea_v2_ethereum.LineaRollup_call_claimMessageWithProof
  WHERE
    call_success = TRUE
  GROUP BY
    1
), aggregated_transfers AS (
  SELECT
    day,
    SUM(CASE WHEN eth_amount > 0 THEN eth_amount ELSE 0 END) AS total_deposits_eth,
    SUM(CASE WHEN eth_amount < 0 THEN -eth_amount ELSE 0 END) AS total_withdrawals_eth,
    SUM(eth_amount) AS net_value_change_eth
  FROM bridge_transfers
  GROUP BY
    day
), cumulative_net_value AS (
  SELECT
    day,
    total_deposits_eth,
    -1 * total_withdrawals_eth AS total_withdrawals_eth,
    net_value_change_eth,
    SUM(net_value_change_eth) OVER (ORDER BY day) AS cumulative_total_eth
  FROM aggregated_transfers
)
SELECT
  *
FROM cumulative_net_value
ORDER BY
  day DESC
