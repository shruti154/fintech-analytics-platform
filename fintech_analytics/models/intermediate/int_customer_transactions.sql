-- Enriched transaction-level table: every transaction row paired with its
-- customer profile. This is the single join point for all downstream marts.
-- No aggregation happens here — that is the marts' responsibility.

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

joined as (
    select
        -- transaction fields
        t.transaction_id,
        t.customer_id,
        t.transaction_date,
        t.transaction_time,
        t.amount,
        t.merchant_category,
        t.merchant_name,
        t.city                                                      as transaction_city,
        t.is_fraud,
        t.status,

        -- customer profile fields
        c.full_name,
        c.email,
        c.account_type,
        c.credit_score,
        c.annual_income,
        c.signup_date,
        c.is_active                                                 as customer_is_active,

        -- derived fields
        date_diff('day', c.signup_date, t.transaction_date)        as days_since_signup,
        case
            when c.credit_score >= 750 then 'excellent'
            when c.credit_score >= 670 then 'good'
            when c.credit_score >= 580 then 'fair'
            else 'poor'
        end                                                         as credit_band

    from transactions t
    inner join customers c
        on t.customer_id = c.customer_id
)

select * from joined
