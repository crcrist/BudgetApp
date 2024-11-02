import os
import sqlite3
import pandas as pd
import streamlit as st
import logging
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

# Load env
load_dotenv()

# Connect to DB
db_path = os.getenv("DB_PATH")

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    logging.info("Connected to the database.")
except sqlite3.Error as e:
    logging.error(f"Database connection failed: {e}")
    exit()



# Enhanced data loading with basic preprocessing
@st.cache_data
def load_data():
    query = "SELECT * FROM transactions"
    df = pd.read_sql(query, conn)

    # print("Unique date formats in posting_date:")
    # print(df['posting_date'].unique())

    # Convert dates using mixed format parsing
    df['posting_date'] = pd.to_datetime(df['posting_date'], format='mixed')
    df['month_year'] = df['posting_date'].dt.to_period('M')

    # ensure all amounts are numeric
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df['balance'] = pd.to_numeric(df['balance'], errors='coerce')

    return df

df = load_data()

# Sidebar for global filters
st.sidebar.header("Filters")
# Date range filter
date_range = st.sidebar.date_input(
    "Select Date Range",
    value=(df['posting_date'].min(), df['posting_date'].max()),
    max_value=datetime.now()
)

# Category filter in sidebar
all_categories = ['All'] + list(df['transaction_category'].unique())
selected_category_filter = st.sidebar.selectbox(
    "Filter by Category",
    options=all_categories
)

# Main dashboard
st.title("Personal Finance Dashboard")

# Filter data based on date range and category
mask = (df['posting_date'].dt.date >= date_range[0]) & (df['posting_date'].dt.date <= date_range[1])
filtered_df = df[mask]

if selected_category_filter != 'All':
    filtered_df = filtered_df[filtered_df['transaction_category'] == selected_category_filter]

all_accounts = ['All'] + list(df['account_owner'].unique())
selected_account = st.sidebar.selectbox(
    "Filter by Account",
    options=all_accounts
)

# Update your filtering logic
if selected_account != 'All':
    filtered_df = filtered_df[filtered_df['account_owner'] == selected_account]

# Summary metrics
st.header("Summary Metrics")
col1, col2, col3, col4 = st.columns(4)

with col1:
    total_spending = filtered_df[filtered_df['amount'] < 0]['amount'].sum()
    st.metric("Total Spending", f"${abs(total_spending):,.2f}")
    
with col2:
    total_income = filtered_df[filtered_df['amount'] > 0]['amount'].sum()
    st.metric("Total Income", f"${total_income:,.2f}")
    
with col3:
    net_flow = total_income + total_spending
    st.metric("Net Cash Flow", f"${net_flow:,.2f}")

with col4:
    # Calculate avg_daily_spending based only on days with expenses
    spending_days = filtered_df[filtered_df['amount'] < 0]['posting_date'].dt.date.nunique()
    if spending_days > 0:
        avg_daily_spending = abs(total_spending) / spending_days
    else:
        avg_daily_spending = 0
    st.metric("Avg. Daily Spending", f"${avg_daily_spending:,.2f}")

# Monthly Spending Trends
st.header("Monthly Spending Analysis")
# Filter only expense transactions (negative amounts)
monthly_expenses = filtered_df[filtered_df['amount'] < 0]

# Group by month and calculate total spending (expenses only) and transaction count
monthly_spending = monthly_expenses.groupby(monthly_expenses['posting_date'].dt.to_period('M')).agg(
    total_amount=('amount', 'sum'),
    transaction_count=('amount', 'count')
).reset_index()

# Plot monthly spending and transaction count
fig_monthly = go.Figure()
fig_monthly.add_trace(go.Bar(
    x=monthly_spending['posting_date'].astype(str),
    y=abs(monthly_spending['total_amount']),  # Taking absolute value for spending
    name='Total Spending'
))
fig_monthly.add_trace(go.Scatter(
    x=monthly_spending['posting_date'].astype(str),
    y=monthly_spending['transaction_count'],
    name='Transaction Count',
    yaxis='y2'
))
fig_monthly.update_layout(
    title='Monthly Spending and Transaction Count',
    yaxis=dict(title='Total Spending ($)'),
    yaxis2=dict(title='Transaction Count', overlaying='y', side='right')
)
st.plotly_chart(fig_monthly)

# Category Analysis with Top 5 + Other
st.header("Category Analysis")
col1, col2 = st.columns(2)

with col1:
    # Get spending by category
    category_spending = filtered_df[filtered_df['amount'] < 0].groupby('transaction_category')['amount'].sum().abs()
    
    # Get top 5 categories
    top_5_categories = category_spending.nlargest(5)
    
    # Calculate "Other" category
    other_amount = category_spending[~category_spending.index.isin(top_5_categories.index)].sum()
    
    # Create new series with Top 5 + Other
    pie_data = pd.concat([top_5_categories, pd.Series({'Other': other_amount})])
    
    # Create pie chart
    fig_category = px.pie(
        values=pie_data.values,
        names=pie_data.index,
        title="Spending by Category (Top 5 + Other)"
    )
    st.plotly_chart(fig_category)

with col2:
    # Top spending categories bar chart
    fig_top = px.bar(
        x=top_5_categories.index,
        y=top_5_categories.values,
        title="Top 5 Spending Categories"
    )
    st.plotly_chart(fig_top)

# Transaction Table
st.header("Transaction Details")

# Amount filter
amount_range = st.slider(
    "Filter by Amount Range",
    float(df['amount'].min()),
    float(df['amount'].max()),
    (float(df['amount'].min()), float(df['amount'].max()))
)

# Apply amount filter to table data
table_data = filtered_df[
    (filtered_df['amount'] >= amount_range[0]) &
    (filtered_df['amount'] <= amount_range[1])
]

# Display filtered transactions
st.dataframe(
    table_data[['posting_date', 'description', 'amount', 'transaction_category', 'type']].sort_values('posting_date', ascending=False),
    use_container_width=True
)

# Monthly Budget Analysis
st.header("Budget Analysis")
monthly_budget = st.number_input("Set Monthly Budget Target ($)", min_value=0.0, value=5000.0)

# Calculate monthly spending vs budget
monthly_vs_budget = filtered_df[filtered_df['amount'] < 0].groupby(
    filtered_df['posting_date'].dt.to_period('M')
)['amount'].sum().abs()

fig_budget = go.Figure()
fig_budget.add_trace(go.Bar(
    x=monthly_vs_budget.index.astype(str),
    y=monthly_vs_budget.values,
    name='Actual Spending'
))
fig_budget.add_trace(go.Scatter(
    x=monthly_vs_budget.index.astype(str),
    y=[monthly_budget] * len(monthly_vs_budget),
    name='Budget Target',
    line=dict(color='red', dash='dash')
))
fig_budget.update_layout(title='Monthly Spending vs Budget Target')
st.plotly_chart(fig_budget)

# Close the database connection
conn.close()