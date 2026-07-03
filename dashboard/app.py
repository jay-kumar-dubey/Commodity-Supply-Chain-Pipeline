import streamlit as st
import duckdb
import plotly.express as px
import pandas as pd
import os

# Page config
st.set_page_config(
    page_title="Commodity Supply Chain Stress Signal",
    page_icon="🛢️",
    layout="wide"
)

# Connect to DuckDB
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'commodity_pipeline.duckdb')

@st.cache_data
def load_data():
    conn = duckdb.connect(DB_PATH, read_only=True)
    df = conn.execute("SELECT * FROM main.gold_stress_signal ORDER BY month DESC").fetchdf()
    conn.close()
    return df

df = load_data()

# Header
st.title("🛢️ Commodity Supply Chain Stress Signal")
st.markdown("Tracks WTI crude oil prices and shipping cost trends to surface early supply chain stress indicators.")

# Metrics row - three columns
col1, col2, col3 = st.columns(3)

with col1:
    latest = df.iloc[0]  # or df.iloc[-1] depending on sort order
    st.metric(label="WTI Oil Price (Latest Month)", value=f"{latest['avg_oil_price']:.2f}", delta=f"{latest['oil_mom_change_pct']:.2f}%")

with col2:
    st.metric(label="Shipping Index (Latest Month)", value=f"{latest['shipping_index']:.2f}", delta=f"{latest['shipping_mom_change_pct']:.2f}%")

with col3:
    stress = latest['stress_signal']
    st.metric(
        label="Stress Score", 
        value=f"{stress:.2f}",
        delta=None
    )

st.divider()

# Charts - two columns
col_left, col_right = st.columns(2)

with col_left:
    fig = px.line(df, x='month', y='avg_oil_price', title='WTI Crude Oil Price (Monthly Avg, $/BBL)')
    st.plotly_chart(fig)

with col_right:
    fig = px.line(df, x='month', y='shipping_index', title='Shipping Cost Index (PPIFIS)')
    st.plotly_chart(fig)

st.divider()

df['color'] = df['stress_signal'].apply(lambda x: 'Stress' if x > 0 else 'Calm')
fig = px.bar(df, x='month', y='stress_signal', color='color', title='Supply Chain Stress Score (MoM)',
             color_discrete_map={'Stress': 'red', 'Calm': 'green'})
st.plotly_chart(fig)

# Data table
st.subheader("Raw Data")
st.dataframe(df)