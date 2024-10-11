WITH data AS (
SELECT 
txs.to AS address
, SUM((txs.gas_used*txs.gas_price)/1e18) AS gas_spend
, SUM((txs.gas_used*txs.gas_price)/1e18 * pu.price) AS gas_spend_usd
, CAST(COUNT(*) AS double) AS tx_count
FROM linea.transactions txs
LEFT JOIN prices.usd pu 
    ON pu.blockchain is null
    AND pu.symbol = 'ETH'
    AND pu.minute = date_trunc('minute', txs.block_time)
    AND pu.minute > NOW() - interval '30' day
WHERE txs.block_time > NOW() - interval '30' day
GROUP BY 1
ORDER BY 2 DESC
LIMIT 100
)

, total AS (
    SELECT SUM((txs.gas_used*txs.gas_price)/1e18) AS gas_used
    , COUNT(*) AS tx_count
    FROM linea.transactions txs
    WHERE txs.block_time > NOW() - interval '30' day AND gas_price > 0
    )

SELECT 
ROW_NUMBER() OVER (ORDER BY d.gas_spend DESC) AS ranking,
l.name AS project,
get_href('https://lineascan.build/' || 'address/' || CAST(d.address AS varchar), "LEFT"(CAST(d.address AS varchar),  5) || '...' || "RIGHT"(CAST(d.address AS varchar),  4)) AS address
, d.gas_spend
, d.gas_spend_usd
-- , d.gas_spend/(SELECT SUM(gas_spend) FROM total) AS percentage_of_gas_used
, d.tx_count
-- , d.tx_count/(SELECT SUM(tx_count) FROM total) AS percentage_of_txs
FROM data d
LEFT JOIN query_3619316 l ON l.address = cast(d.address as varchar) 
WHERE d.address IS NOT NULL
