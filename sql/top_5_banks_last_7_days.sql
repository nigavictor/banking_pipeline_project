SELECT 
    bank_name,
    SUM(amount) AS total_transaction_volume
FROM 
    transactions
WHERE 
    timestamp >= NOW() - INTERVAL 7 DAY
GROUP BY 
    bank_name
ORDER BY 
    total_transaction_volume DESC
LIMIT 5;

