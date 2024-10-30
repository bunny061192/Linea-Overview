WITH
  raw_tx_tbl AS (
    SELECT
      DATE(block_time) AS tx_date,
      hash,
      "from" AS from_address,
      success,
      (gas_price * gas_used) / 1e18 AS tx_fee_eth,
      p.price * (gas_price * gas_used) / 1e18 AS tx_fee_usd
    FROM linea.transactions
    LEFT JOIN prices.usd p ON p.minute = DATE_TRUNC('minute', block_time)
        AND p.blockchain = 'ethereum'
        AND p.symbol = 'WETH'
    WHERE block_time >= CURRENT_DATE - INTERVAL '90' day
  ),

  transactions_tbl AS (
    SELECT
      tx_date,
      COUNT(hash) AS total_tx,
      COUNT(CASE WHEN success = true THEN 1 END) AS successful_tx,
      COUNT(CASE WHEN success = false THEN 1 END) AS failed_tx,
      AVG(tx_fee_eth) AS tx_fee_eth_average,
      APPROX_PERCENTILE(tx_fee_eth, 0.5) AS tx_fee_eth_median,
      APPROX_PERCENTILE(tx_fee_eth, 0.1) AS tx_fee_eth_percentile_10,
      APPROX_PERCENTILE(tx_fee_eth, 0.9) AS tx_fee_eth_percentile_90,
      AVG(tx_fee_usd) AS tx_fee_usd_average,
      APPROX_PERCENTILE(tx_fee_usd, 0.5) AS tx_fee_usd_median,
      APPROX_PERCENTILE(tx_fee_usd, 0.1) AS tx_fee_usd_percentile_10,
      APPROX_PERCENTILE(tx_fee_usd, 0.9) AS tx_fee_usd_percentile_90
    FROM raw_tx_tbl
    GROUP BY 1    
  ),
  
  new_addresses_tbl AS (
    SELECT
      tx_date,
      COUNT(DISTINCT from_address) AS new_addresses
    FROM (
      SELECT 
        tx_date,
        from_address,
        MIN(tx_date) OVER (PARTITION BY from_address) AS first_tx_date
      FROM raw_tx_tbl
    )
    WHERE tx_date = first_tx_date
    GROUP BY 1
  ),
  
  cumulative_addresses_tbl AS (
    SELECT
      tx_date,
      SUM(new_addresses) OVER (ORDER BY tx_date ASC) AS cumulative_addresses
    FROM new_addresses_tbl
  )

SELECT
  t.tx_date,
  t.total_tx,
  t.successful_tx,
  t.failed_tx,
  n.new_addresses,
  c.cumulative_addresses,
  t.tx_fee_eth_average,
  t.tx_fee_eth_median,
  t.tx_fee_eth_percentile_10,
  t.tx_fee_eth_percentile_90,
  t.tx_fee_usd_average,
  t.tx_fee_usd_median,
  t.tx_fee_usd_percentile_10,
  t.tx_fee_usd_percentile_90
FROM transactions_tbl t
LEFT JOIN new_addresses_tbl n ON t.tx_date = n.tx_date
LEFT JOIN cumulative_addresses_tbl c ON t.tx_date = c.tx_date
ORDER BY t.tx_date DESC;
