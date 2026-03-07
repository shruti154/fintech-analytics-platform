import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

DB_PATH = "../fintech.duckdb"

st.set_page_config(page_title="Fintech Analytics", layout="wide")
st.title("Fintech Analytics Dashboard")
st.caption("50,000 transactions · 10,000 customers · Jan 2023 – Dec 2024")


@st.cache_data
def load(query):
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute(query).df()
    con.close()
    return df


monthly   = load("SELECT * FROM mart_monthly_trends ORDER BY year_month")
category  = load("SELECT * FROM mart_category_analysis ORDER BY total_spend DESC")
segments  = load("SELECT * FROM mart_customer_segments")
fraud     = load("SELECT * FROM mart_fraud_analysis")

# ── KPIs ─────────────────────────────────────────────────────────────────────
total_spend   = monthly["total_spend"].sum()
total_txns    = monthly["total_transactions"].sum()
avg_fraud_pct = monthly["fraud_rate_pct"].mean()
unique_custs  = segments["customer_id"].nunique()

k1, k2, k3, k4 = st.columns(4)
k1.metric("Total Spend",        f"£{total_spend:,.0f}")
k2.metric("Transactions",       f"{total_txns:,.0f}")
k3.metric("Avg Fraud Rate",     f"{avg_fraud_pct:.2f}%")
k4.metric("Customers",          f"{unique_custs:,}")

st.divider()

# ── Monthly Spend Trend ───────────────────────────────────────────────────────
st.subheader("Monthly Spend & Fraud Rate")

fig = go.Figure()
fig.add_trace(go.Bar(
    x=monthly["year_month"], y=monthly["total_spend"],
    name="Total Spend (£)", marker_color="#4C78A8", yaxis="y1"
))
fig.add_trace(go.Scatter(
    x=monthly["year_month"], y=monthly["fraud_rate_pct"],
    name="Fraud Rate (%)", mode="lines+markers",
    line=dict(color="#E45756", width=2), yaxis="y2"
))
fig.update_layout(
    yaxis=dict(title="Spend (£)"),
    yaxis2=dict(title="Fraud Rate (%)", overlaying="y", side="right", showgrid=False),
    legend=dict(orientation="h", y=1.1),
    height=350, margin=dict(t=20, b=20)
)
st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Category Analysis ─────────────────────────────────────────────────────────
st.subheader("Spend & Fraud by Category")

col1, col2 = st.columns(2)

with col1:
    fig_spend = px.bar(
        category, x="total_spend", y="merchant_category",
        orientation="h", labels={"total_spend": "Total Spend (£)", "merchant_category": ""},
        color="total_spend", color_continuous_scale="Blues"
    )
    fig_spend.update_layout(
        coloraxis_showscale=False, height=380,
        margin=dict(t=10, b=10), yaxis=dict(autorange="reversed")
    )
    st.plotly_chart(fig_spend, use_container_width=True)

with col2:
    fig_fraud = px.bar(
        category.sort_values("fraud_rate_pct", ascending=True),
        x="fraud_rate_pct", y="merchant_category",
        orientation="h", labels={"fraud_rate_pct": "Fraud Rate (%)", "merchant_category": ""},
        color="fraud_rate_pct", color_continuous_scale="Reds"
    )
    fig_fraud.update_layout(
        coloraxis_showscale=False, height=380,
        margin=dict(t=10, b=10), yaxis=dict(autorange="reversed")
    )
    st.plotly_chart(fig_fraud, use_container_width=True)

st.divider()

# ── Customer Segments ─────────────────────────────────────────────────────────
st.subheader("Customer Segments")

col3, col4 = st.columns(2)

with col3:
    seg_counts = segments["customer_segment"].value_counts().reset_index()
    seg_counts.columns = ["segment", "count"]
    order = ["high_value", "mid_value", "low_value"]
    seg_counts["segment"] = pd.Categorical(seg_counts["segment"], categories=order, ordered=True)
    seg_counts = seg_counts.sort_values("segment")

    fig_seg = px.pie(
        seg_counts, names="segment", values="count",
        color="segment",
        color_discrete_map={
            "high_value": "#2196F3",
            "mid_value":  "#64B5F6",
            "low_value":  "#BBDEFB"
        },
        hole=0.45
    )
    fig_seg.update_layout(height=320, margin=dict(t=10, b=10))
    st.plotly_chart(fig_seg, use_container_width=True)

with col4:
    seg_spend = (
        segments.groupby("customer_segment")["total_spend"]
        .agg(["mean", "median"])
        .reset_index()
        .rename(columns={"mean": "Avg Spend", "median": "Median Spend", "customer_segment": "Segment"})
    )
    seg_spend["Segment"] = pd.Categorical(seg_spend["Segment"], categories=order, ordered=True)
    seg_spend = seg_spend.sort_values("Segment")

    fig_box = px.box(
        segments, x="customer_segment", y="total_spend",
        category_orders={"customer_segment": order},
        labels={"customer_segment": "Segment", "total_spend": "Lifetime Spend (£)"},
        color="customer_segment",
        color_discrete_map={
            "high_value": "#2196F3",
            "mid_value":  "#64B5F6",
            "low_value":  "#BBDEFB"
        }
    )
    fig_box.update_layout(
        showlegend=False, height=320, margin=dict(t=10, b=10)
    )
    st.plotly_chart(fig_box, use_container_width=True)

st.divider()

# ── Fraud Heatmap ─────────────────────────────────────────────────────────────
st.subheader("Fraud Rate by Account Type × Category")

pivot = fraud.pivot(index="merchant_category", columns="account_type", values="fraud_rate_pct")

fig_heat = px.imshow(
    pivot,
    color_continuous_scale="Reds",
    labels=dict(color="Fraud Rate (%)"),
    aspect="auto"
)
fig_heat.update_layout(height=400, margin=dict(t=10, b=10))
st.plotly_chart(fig_heat, use_container_width=True)
