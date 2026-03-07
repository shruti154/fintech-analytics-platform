with source as (
    select * from read_csv_auto(
        '/Users/shruti/fintech_analytics_platform/data/raw/customers.csv'
    )
),

staged as (
    select
        customer_id,
        first_name,
        last_name,
        lower(first_name || ' ' || last_name)       as full_name,
        lower(email)                                 as email,
        city,
        lower(account_type)                          as account_type,
        cast(signup_date as date)                    as signup_date,
        cast(credit_score as integer)                as credit_score,
        round(cast(annual_income as decimal(12,2)), 2) as annual_income,
        cast(is_active as boolean)                   as is_active
    from source
)

select * from staged
