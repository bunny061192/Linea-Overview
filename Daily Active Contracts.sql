SELECT 
    DATE(t.block_time) AS day,
    COUNT(DISTINCT t.to) AS active_contracts
FROM linea.transactions t
INNER JOIN linea.creation_traces c ON c.address = t.to
WHERE t.success = TRUE
    AND t.block_time >= CURRENT_DATE - INTERVAL '90' day
GROUP BY 1
ORDER BY day DESC;
