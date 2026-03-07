-- One row per customer. Aggregates transaction history into spending profiles
-- and assigns a value segment (high/mid/low) using decile ranking.

with customer_transactions as (
    select * from {{ ref('int_customer_transactions') }}
),

customer_metrics as (
    select
        customer_id,
        max(full_name)                                              as full_name,
        max(email)                                                  as email,
        max(account_type)                                           as account_type,
        max(credit_score)                                           as credit_score,
        max(credit_band)                                            as credit_band,
        max(annual_income)                                          as annual_income,
        max(customer_is_active)                                     as is_active,

        -- activity metrics
        count(*)                                                    as total_transactions,
        round(sum(amount), 2)                                       as total_spend,
        round(avg(amount), 2)                                       as avg_transaction_value,
        round(max(amount), 2)                                       as max_transaction_value,

        -- tenure
        min(transaction_date)                                       as first_transaction_date,
        max(transaction_date)                                       as last_transaction_date,
        date_diff('day', min(transaction_date), max(transaction_date))  as active_days,

        -- category behaviour
        count(distinct merchant_category)                           as unique_categories_used,
        mode(merchant_category)                                     as top_spend_category,

        -- fraud exposure
        sum(case when is_fraud then 1 else 0 end)                   as fraud_transaction_count,
        round(
            sum(case when is_fraud then 1 else 0 end) * 100.0 / count(*),
            2
        )                                                           as fraud_rate_pct

    from customer_transactions
    group by customer_id
),

-- Use a separate CTE for ntile so we don't repeat the window expression
with_decile as (
    select
        *,
        ntile(10) over (order by total_spend desc)                  as spend_decile
    from customer_metrics
),

segmented as (
    select
        *,
        case
            when spend_decile <= 2  then 'high_value'
            when spend_decile <= 7  then 'mid_value'
            else                         'low_value'
        end                                                         as customer_segment
    from with_decile
)

select
    customer_id,
    full_name,
    email,
    account_type,
    credit_score,
    credit_band,
    annual_income,
    is_active,
    total_transactions,
    total_spend,
    avg_transaction_value,
    max_transaction_value,
    first_transaction_date,
    last_transaction_date,
    active_days,
    unique_categories_used,
    top_spend_category,
    fraud_transaction_count,
    fraud_rate_pct,
    spend_decile,
    customer_segment
from segmented
order by total_spend desc
