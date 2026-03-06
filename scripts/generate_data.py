import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os

fake = Faker('en_GB')
np.random.seed(42)
random.seed(42)

# ── CONFIG ──────────────────────────────────────────
NUM_CUSTOMERS    = 10000
NUM_TRANSACTIONS = 50000
START_DATE       = datetime(2023, 1, 1)
END_DATE         = datetime(2024, 12, 31)

MERCHANT_CATEGORIES = [
    'groceries', 'eating_out', 'transport', 'entertainment',
    'shopping', 'travel', 'utilities', 'healthcare',
    'subscriptions', 'cash_withdrawal'
]

ACCOUNT_TYPES = ['current', 'savings', 'premium']

# ── CUSTOMERS ────────────────────────────────────────
print("Generating customers...")
customers = []
for i in range(NUM_CUSTOMERS):
    account_type = random.choices(
        ACCOUNT_TYPES, weights=[0.6, 0.3, 0.1]
    )[0]
    customers.append({
        'customer_id':    f'CUST_{i+1:05d}',
        'first_name':     fake.first_name(),
        'last_name':      fake.last_name(),
        'email':          fake.email(),
        'city':           fake.city(),
        'account_type':   account_type,
        'signup_date':    fake.date_between(
                              start_date='-5y', end_date='-6m'
                          ),
        'credit_score':   np.random.randint(300, 850),
        'annual_income':  round(np.random.normal(45000, 20000), 2),
        'is_active':      random.choices([True, False],
                              weights=[0.85, 0.15])[0]
    })

customers_df = pd.DataFrame(customers)
print(f"  ✓ {len(customers_df)} customers created")

# ── TRANSACTIONS ─────────────────────────────────────
print("Generating transactions...")
transactions = []
for i in range(NUM_TRANSACTIONS):
    customer_id = random.choice(customers_df['customer_id'].tolist())
    category    = random.choice(MERCHANT_CATEGORIES)
    date        = START_DATE + timedelta(
                      seconds=random.randint(
                          0, int((END_DATE - START_DATE).total_seconds())
                      )
                  )

    # Realistic amounts per category
    amount_map = {
        'groceries':       (10,  200),
        'eating_out':      (5,   120),
        'transport':       (2,   80),
        'entertainment':   (5,   150),
        'shopping':        (10,  500),
        'travel':          (50,  2000),
        'utilities':       (30,  300),
        'healthcare':      (10,  500),
        'subscriptions':   (5,   50),
        'cash_withdrawal': (20,  500)
    }
    lo, hi   = amount_map[category]
    amount   = round(random.uniform(lo, hi), 2)

    # Fraud logic — small % of high-value transactions
    is_fraud = (
        amount > 800 and random.random() < 0.08
    ) or random.random() < 0.005

    transactions.append({
        'transaction_id':  f'TXN_{i+1:07d}',
        'customer_id':     customer_id,
        'date':            date.strftime('%Y-%m-%d'),
        'time':            date.strftime('%H:%M:%S'),
        'amount':          amount,
        'merchant_category': category,
        'merchant_name':   fake.company(),
        'city':            fake.city(),
        'is_fraud':        is_fraud,
        'status':          random.choices(
                               ['completed', 'pending', 'failed'],
                               weights=[0.92, 0.05, 0.03]
                           )[0]
    })

transactions_df = pd.DataFrame(transactions)
print(f"  ✓ {len(transactions_df)} transactions created")

# ── SAVE ─────────────────────────────────────────────
print("Saving to data/raw/...")
os.makedirs('data/raw', exist_ok=True)
customers_df.to_csv('data/raw/customers.csv',       index=False)
transactions_df.to_csv('data/raw/transactions.csv', index=False)

print("\n✅ Done!")
print(f"   customers.csv    → {len(customers_df):,} rows")
print(f"   transactions.csv → {len(transactions_df):,} rows")


