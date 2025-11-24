
-- fact_transactions.sql
WITH source AS (
    -- SELECT * FROM {{ source('raw_data', 'transactions') }}
    SELECT 
        'tx_1' as transaction_id, 'user_1' as user_id, 100.0 as amount, 'USD' as currency, '2023-01-01' as transaction_date
    UNION ALL
    SELECT 
        'tx_2' as transaction_id, 'user_2' as user_id, 200.0 as amount, 'EUR' as currency, '2023-01-02' as transaction_date
)

SELECT
    t.transaction_id,
    t.user_id,
    t.amount,
    t.currency,
    t.transaction_date,
    current_timestamp as loaded_at
FROM source t
