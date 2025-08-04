SELECT 
    customer_id,
    bank_name,
    DATE_FORMAT(timestamp, '%M-%Y') AS transaction_month,
    ROUND(AVG(amount), 2) AS avg_transaction_value
FROM 
    transactions
WHERE 
    DATE_FORMAT(timestamp, '%Y-%m') = '2025-08'
GROUP BY 
    customer_id,
    bank_name,
    transaction_month;

