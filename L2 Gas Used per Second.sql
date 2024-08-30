SELECT
  DATE(block_time) AS day,
  SUM(gas_used) / 86400 AS gas_used_per_second
FROM linea.transactions
WHERE
  block_time >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY
  1
ORDER BY
  day
