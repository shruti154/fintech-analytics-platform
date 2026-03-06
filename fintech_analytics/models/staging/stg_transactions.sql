with source as (
    select * from read_csv_auto(
        '/Users/shruti/fintech_analytics_platform/data/raw/transactions.csv'
    )
),

staged as (
    select
        transaction_id,
        customer_id,
        cast(date as date)                          as transaction_date,
        cast(time as time)                          as transaction_time,
        cast(amount as decimal(10,2))               as amount,
        lower(merchant_category)                    as merchant_category,
        merchant_name,
        city,
        cast(is_fraud as boolean)                   as is_fraud,
        lower(status)                               as status
    from source
    where status != 'failed'
)

select * from staged

