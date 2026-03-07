with transactions as (
    select * from {{ ref('stg_transactions') }}
),

category_metrics as (
    select
        merchant_category,
        count(*)                                    as total_transactions,
        round(sum(amount), 2)                       as total_spend,
        round(avg(amount), 2)                       as avg_transaction_value,
        sum(case when is_fraud then 1 else 0 end)   as fraud_count,
        round(
            sum(case when is_fraud then 1 else 0 end) * 100.0 / count(*),
            2
        )                                           as fraud_rate_pct
    from transactions
    group by merchant_category
)

select
    merchant_category,
    total_transactions,
    total_spend,
    avg_transaction_value,
    fraud_count,
    fraud_rate_pct
from category_metrics
order by total_spend desc


