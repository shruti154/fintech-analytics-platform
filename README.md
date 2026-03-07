# Fintech Analytics Platform

An end-to-end analytics engineering portfolio project built with dbt and DuckDB.
Transforms 50,000 synthetic UK banking transactions into a layered data model
with data quality tests and business-ready analytics tables.

---

## Tech Stack

| Tool | Role |
|------|------|
| Python + Faker | Synthetic data generation |
| DuckDB | Local OLAP engine (no server required) |
| dbt | Data transformation, testing, and documentation |

---

## Architecture

Data flows through three dbt layers, each with a distinct responsibility:

```
Raw Data (data/raw/)
├── transactions.csv   50,000 transaction events, Jan 2023 – Dec 2024
└── customers.csv      10,000 synthetic UK customer profiles

        │
        ▼

Staging Layer  (materialized as views)
├── stg_transactions   Type casting, filter failed transactions
└── stg_customers      Type casting, normalize casing, derive full_name

        │
        ▼

Intermediate Layer  (materialized as views)
└── int_customer_transactions   Join transactions ↔ customers, derive credit_band
                                and days_since_signup. Single join point for all marts.

        │
        ▼

Marts Layer  (materialized as tables)
├── mart_category_analysis    Spend + fraud metrics by merchant category
├── mart_customer_segments    Per-customer profiles with RFM-style metrics
│                             and value segment (high / mid / low)
├── mart_monthly_trends       Time series with MoM growth rates and
│                             cumulative running totals
└── mart_fraud_analysis       Fraud breakdown by account type × category
```

---

## Data Quality

**39 dbt tests** across all layers:

| Layer | Tests |
|-------|-------|
| Staging | 20 tests — uniqueness, nullability, accepted values, referential integrity |
| Intermediate | 7 tests — primary key, nullability, accepted values |
| Marts | 12 tests — primary keys, nullability, accepted values |

Run all tests with:
```bash
dbt test
```

Notable: the `relationships` test on `stg_transactions.customer_id` verifies that every transaction links to a real customer — referential integrity enforced at the transformation layer.

---

## Mart Reference

### `mart_category_analysis`
| Column | Description |
|--------|-------------|
| merchant_category | Spending category (10 categories) |
| total_transactions | Transaction count |
| total_spend | Total GBP spend |
| avg_transaction_value | Mean transaction amount |
| fraud_count | Fraudulent transaction count |
| fraud_rate_pct | Fraud as % of transactions |

### `mart_customer_segments`
| Column | Description |
|--------|-------------|
| customer_id | Customer identifier |
| customer_segment | `high_value` / `mid_value` / `low_value` (spend decile) |
| credit_band | `excellent` / `good` / `fair` / `poor` |
| total_spend | Lifetime spend |
| top_spend_category | Most frequent spending category (mode) |
| fraud_rate_pct | Customer's personal fraud rate |
| active_days | Days between first and last transaction |

### `mart_monthly_trends`
| Column | Description |
|--------|-------------|
| year_month | Calendar month (YYYY-MM) |
| total_spend | Monthly GBP spend |
| spend_growth_pct | Month-over-month spend change % |
| fraud_rate_pct | Monthly fraud rate |
| cumulative_spend | Running total spend from month 1 |

### `mart_fraud_analysis`
| Column | Description |
|--------|-------------|
| account_type | Customer tier (current / savings / premium) |
| merchant_category | Spending category |
| fraud_transactions | Fraud count for this segment × category |
| total_fraud_amount | Total GBP lost to fraud |
| fraud_rate_pct | Fraud rate within this segment × category |

---

## Setup

**Prerequisites:** Python 3.9+, pip, dbt-duckdb

```bash
# 1. Clone the repository
git clone https://github.com/shruti154/fintech-analytics-platform.git
cd fintech-analytics-platform

# 2. Install Python dependencies
pip install pandas numpy faker

# 3. Install dbt
pip install dbt-duckdb

# 4. Configure dbt profile
#    Create ~/.dbt/profiles.yml with:

fintech_analytics:
  outputs:
    dev:
      type: duckdb
      path: /absolute/path/to/fintech-analytics-platform/fintech.duckdb
      threads: 1
  target: dev

# 5. Generate synthetic data
cd scripts
python generate_data.py
cd ..

# 6. Run the pipeline
cd fintech_analytics
dbt run

# 7. Run data quality tests
dbt test
```

---

## Project Structure

```
fintech-analytics-platform/
├── data/
│   └── raw/
│       ├── transactions.csv
│       └── customers.csv
├── fintech_analytics/          # dbt project
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── schema.yml
│   │   │   ├── stg_transactions.sql
│   │   │   └── stg_customers.sql
│   │   ├── intermediate/
│   │   │   ├── schema.yml
│   │   │   └── int_customer_transactions.sql
│   │   └── marts/
│   │       ├── schema.yml
│   │       ├── mart_category_analysis.sql
│   │       ├── mart_customer_segments.sql
│   │       ├── mart_monthly_trends.sql
│   │       └── mart_fraud_analysis.sql
│   └── dbt_project.yml
└── scripts/
    └── generate_data.py
```
