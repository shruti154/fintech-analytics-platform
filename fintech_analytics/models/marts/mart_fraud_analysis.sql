-- Fraud breakdown by account type × merchant category.
-- Answers: which customer segments face the most fraud risk, and in which
-- spending categories? One row per account_type / merchant_category combination.

with enriched as (
    select * from {{ ref('int_customer_transactions') }}
),

-- Aggregate all transactions for the denominator (fraud rate calculation)
all_txns as (
    select
        account_type,
        merchant_category,
        count(*)                                                    as total_transactions,
        round(sum(amount), 2)                                       as total_amount,
        round(avg(amount), 2)                                       as avg_amount
    from enriched
    group by account_type, merchant_category
),

-- Aggregate only fraudulent transactions
fraud_txns as (
    select
        account_type,
        merchant_category,
        count(*)                                                    as fraud_transactions,
        round(sum(amount), 2)                                       as total_fraud_amount,
        round(avg(amount), 2)                                       as avg_fraud_amount,
        round(min(amount), 2)                                       as min_fraud_amount,
        round(max(amount), 2)                                       as max_fraud_amount
    from enriched
    where is_fraud = true
    group by account_type, merchant_category
),

joined as (
    select
        a.account_type,
        a.merchant_category,
        a.total_transactions,
        a.total_amount,
        a.avg_amount,
        coalesce(f.fraud_transactions, 0)                           as fraud_transactions,
        coalesce(f.total_fraud_amount, 0)                           as total_fraud_amount,
        coalesce(f.avg_fraud_amount, 0)                             as avg_fraud_amount,
        coalesce(f.min_fraud_amount, 0)                             as min_fraud_amount,
        coalesce(f.max_fraud_amount, 0)                             as max_fraud_amount,
        round(
            coalesce(f.fraud_transactions, 0) * 100.0 / a.total_transactions,
            2
        )                                                           as fraud_rate_pct
    from all_txns a
    left join fraud_txns f
        on a.account_type       = f.account_type
        and a.merchant_category = f.merchant_category
)

select
    account_type,
    merchant_category,
    total_transactions,
    total_amount,
    avg_amount,
    fraud_transactions,
    total_fraud_amount,
    avg_fraud_amount,
    min_fraud_amount,
    max_fraud_amount,
    fraud_rate_pct
from joined
order by total_fraud_amount desc
