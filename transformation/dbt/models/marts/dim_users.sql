
-- dim_users.sql
WITH source AS (
    -- In a real scenario, this would select from a staging table or raw source
    -- For this example, we assume a 'raw_users' table exists or we mock it
    -- SELECT * FROM {{ source('raw_data', 'users') }}
    SELECT 
        'user_1' as user_id, 'Alice' as name, 'alice@example.com' as email, 'Young' as age_group
    UNION ALL
    SELECT 
        'user_2' as user_id, 'Bob' as name, 'bob@example.com' as email, 'Adult' as age_group
)

SELECT
    user_id,
    name,
    email,
    age_group,
    current_timestamp as loaded_at
FROM source
