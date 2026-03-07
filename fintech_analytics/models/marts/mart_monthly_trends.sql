-- Monthly transaction trends with month-over-month growth rates and
-- running cumulative totals. One row per calendar month.

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

monthly as (
    select
        strftime('%Y-%m', transaction_date)                         as year_month,

        count(*)                                                    as total_transactions,
        round(sum(amount), 2)                                       as total_spend,
        round(avg(amount), 2)                                       as avg_transaction_value,
        count(distinct customer_id)                                 as unique_customers,

        sum(case when is_fraud then 1 else 0 end)                   as fraud_count,
        round(sum(case when is_fraud then amount else 0 end), 2)    as fraud_amount,
        round(
            sum(case when is_fraud then 1 else 0 end) * 100.0 / count(*),
            2
        )                                                           as fraud_rate_pct

    from transactions
    group by strftime('%Y-%m', transaction_date)
),

with_window_metrics as (
    select
        year_month,
        total_transactions,
        total_spend,
        avg_transaction_value,
        unique_customers,
        fraud_count,
        fraud_amount,
        fraud_rate_pct,

        -- month-over-month comparisons using LAG window function
        -- LAG(col) looks at the value from the previous row (ordered by year_month)
        lag(total_spend)        over (order by year_month)          as prev_month_spend,
        lag(total_transactions) over (order by year_month)          as prev_month_transactions,

        round(
            (total_spend - lag(total_spend) over (order by year_month))
            * 100.0
            / nullif(lag(total_spend) over (order by year_month), 0),
            2
        )                                                           as spend_growth_pct,

        round(
            (total_transactions - lag(total_transactions) over (order by year_month))
            * 100.0
            / nullif(lag(total_transactions) over (order by year_month), 0),
            2
        )                                                           as transaction_growth_pct,

        -- running totals: cumulative sum from the first month to the current row
        sum(total_transactions) over (
            order by year_month
            rows between unbounded preceding and current row
        )                                                           as cumulative_transactions,

        round(sum(total_spend) over (
            order by year_month
            rows between unbounded preceding and current row
        ), 2)                                                       as cumulative_spend

    from monthly
)

select
    year_month,
    total_transactions,
    total_spend,
    avg_transaction_value,
    unique_customers,
    fraud_count,
    fraud_amount,
    fraud_rate_pct,
    prev_month_spend,
    prev_month_transactions,
    spend_growth_pct,
    transaction_growth_pct,
    cumulative_transactions,
    cumulative_spend
from with_window_metrics
order by year_month
