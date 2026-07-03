import streamlit as st
import duckdb
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import os
from datetime import datetime

# Page config
st.set_page_config(
    page_title="Commodity Supply Chain Stress Signal",
    page_icon="🛢️",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS
st.markdown("""
<style>
    .main { background-color: #0e1117; }
    .stMetric { background-color: #1e2130; padding: 15px; border-radius: 10px; }
    .status-calm { background-color: #1a3a2a; border-left: 4px solid #00cc66;
                   padding: 15px 20px; border-radius: 6px; margin-bottom: 20px; }
    .status-stress { background-color: #3a1a1a; border-left: 4px solid #ff4444;
                     padding: 15px 20px; border-radius: 6px; margin-bottom: 20px; }
    .insight-box { background-color: #1e2130; padding: 15px; border-radius: 8px;
                   margin-top: 10px; font-size: 0.85em; color: #aaaaaa; }
    h1 { color: #ffffff; }
    .subtitle { color: #888888; font-size: 0.95em; margin-top: -15px; }
</style>
""", unsafe_allow_html=True)

# DB connection
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'commodity_pipeline.duckdb')

@st.cache_data(ttl=3600)
def load_data():
    conn = duckdb.connect(DB_PATH, read_only=True)
    df = conn.execute("""
        SELECT * FROM main.gold_stress_signal
        ORDER BY month ASC
    """).fetchdf()
    conn.close()
    df['month'] = pd.to_datetime(df['month'])
    return df

df = load_data()
latest = df.iloc[-1]
prev = df.iloc[-2]

# Determine status
stress_val = latest['stress_signal']
STRESS_THRESHOLD = 2.0
is_stressed = stress_val > STRESS_THRESHOLD

# Historical context
hist_avg_stress = df['stress_signal'].mean()
stress_percentile = (df['stress_signal'] < stress_val).mean() * 100

# ── HEADER ────────────────────────────────────────────────────────────────────
st.title("🛢️ Commodity Supply Chain Stress Signal")
st.markdown(
    '<p class="subtitle">Real-time monitoring of WTI crude oil prices and global shipping costs '
    '— surfacing supply chain stress 2–3 weeks ahead of market movement.</p>',
    unsafe_allow_html=True
)
st.markdown(
    f"*Last updated: {datetime.now().strftime('%B %d, %Y')} "
    f"· Data: EIA (daily WTI) + FRED PPIFIS (monthly shipping)*"
)

st.divider()

# ── STATUS BANNER ─────────────────────────────────────────────────────────────
if is_stressed:
    st.markdown(f"""
    <div class="status-stress">
        <strong>🔴 SUPPLY CHAIN STRESS DETECTED</strong><br>
        Shipping costs are rising faster than oil prices — historically precedes supply disruptions 2–3 weeks ahead.
        Current stress score: <strong>{stress_val:.2f}</strong> (above threshold of {STRESS_THRESHOLD})
    </div>
    """, unsafe_allow_html=True)
else:
    st.markdown(f"""
    <div class="status-calm">
        <strong>🟢 SUPPLY CHAINS CALM</strong><br>
        Shipping cost growth is not outpacing oil price movement. No immediate stress signal detected.
        Current stress score: <strong>{stress_val:.2f}</strong> (below threshold of {STRESS_THRESHOLD})
    </div>
    """, unsafe_allow_html=True)

# ── METRICS ROW ───────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="🛢️ WTI Oil Price",
        value=f"${latest['avg_oil_price']:.2f}/bbl",
        delta=f"{latest['oil_mom_change_pct']:.2f}% MoM",
        delta_color="inverse"
    )
    st.markdown(
        '<div class="insight-box">Monthly average WTI crude spot price at Cushing, Oklahoma. '
        'Rising prices indicate supply tightness or demand surge.</div>',
        unsafe_allow_html=True
    )

with col2:
    st.metric(
        label="🚢 Shipping Cost Index",
        value=f"{latest['shipping_index']:.1f}",
        delta=f"{latest['shipping_mom_change_pct']:.2f}% MoM",
        delta_color="inverse"
    )
    st.markdown(
        '<div class="insight-box">FRED PPIFIS — Producer Price Index for Transportation & Warehousing. '
        'Baseline 100 = Nov 2009. Rising = more expensive to move goods.</div>',
        unsafe_allow_html=True
    )

with col3:
    st.metric(
        label="⚡ Stress Signal",
        value=f"{stress_val:.2f}",
        delta=f"{stress_val - prev['stress_signal']:.2f} vs last month",
        delta_color="inverse"
    )
    st.markdown(
        f'<div class="insight-box">Stress = Shipping MoM% − Oil MoM%. Positive = stress building. '
        f'Threshold: {STRESS_THRESHOLD}. Current percentile: {stress_percentile:.0f}th of all months tracked.</div>',
        unsafe_allow_html=True
    )

with col4:
    months_in_stress = (df['stress_signal'] > STRESS_THRESHOLD).sum()
    total_months = len(df)
    st.metric(
        label="📊 Historical Stress Rate",
        value=f"{months_in_stress}/{total_months} months",
        delta=f"Avg signal: {hist_avg_stress:.2f}"
    )
    st.markdown(
        '<div class="insight-box">How often has the stress threshold been breached historically? '
        'Higher frequency = more volatile supply environment.</div>',
        unsafe_allow_html=True
    )

st.divider()

# ── MAIN CHARTS ───────────────────────────────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    fig_oil = px.line(
        df, x='month', y='avg_oil_price',
        title='WTI Crude Oil Price (Monthly Avg, $/BBL)',
        labels={'avg_oil_price': 'Price ($/BBL)', 'month': ''},
        color_discrete_sequence=['#4fc3f7']
    )
    fig_oil.add_hline(
        y=df['avg_oil_price'].mean(),
        line_dash="dot",
        line_color="#888888",
        annotation_text=f"Avg: ${df['avg_oil_price'].mean():.2f}",
        annotation_position="top right"
    )
    fig_oil.update_layout(
        plot_bgcolor='#1e2130',
        paper_bgcolor='#1e2130',
        font_color='#cccccc',
        hovermode='x unified'
    )
    st.plotly_chart(fig_oil, use_container_width=True)

with col_right:
    fig_ship = px.line(
        df, x='month', y='shipping_index',
        title='Shipping Cost Index — FRED PPIFIS',
        labels={'shipping_index': 'Index (Nov 2009 = 100)', 'month': ''},
        color_discrete_sequence=['#81c784']
    )
    fig_ship.add_hline(
        y=df['shipping_index'].mean(),
        line_dash="dot",
        line_color="#888888",
        annotation_text=f"Avg: {df['shipping_index'].mean():.1f}",
        annotation_position="top right"
    )
    fig_ship.update_layout(
        plot_bgcolor='#1e2130',
        paper_bgcolor='#1e2130',
        font_color='#cccccc',
        hovermode='x unified'
    )
    st.plotly_chart(fig_ship, use_container_width=True)

# ── STRESS SIGNAL CHART ───────────────────────────────────────────────────────
st.subheader("⚡ Supply Chain Stress Signal — Month over Month")
st.caption(
    "Positive (red) = shipping costs rising faster than oil prices → stress building. "
    "Negative (green) = calm. Yellow dashed line = stress threshold."
)

df['signal_color'] = df['stress_signal'].apply(
    lambda x: 'Stress' if x > STRESS_THRESHOLD else 'Calm'
)

fig_stress = px.bar(
    df, x='month', y='stress_signal',
    color='signal_color',
    color_discrete_map={'Stress': '#ff4444', 'Calm': '#00cc66'},
    labels={'stress_signal': 'Stress Score', 'month': '', 'signal_color': 'Status'}
)
fig_stress.add_hline(
    y=STRESS_THRESHOLD,
    line_dash="dash",
    line_color="#ffaa00",
    line_width=2,
    annotation_text=f"Stress Threshold ({STRESS_THRESHOLD})",
    annotation_position="top right",
    annotation_font_color="#ffaa00"
)
fig_stress.add_hline(y=0, line_color="#555555", line_width=1)
fig_stress.update_layout(
    plot_bgcolor='#1e2130',
    paper_bgcolor='#1e2130',
    font_color='#cccccc',
    hovermode='x unified',
    height=350
)
st.plotly_chart(fig_stress, use_container_width=True)

# ── MOM COMPARISON CHART ──────────────────────────────────────────────────────
st.subheader("📈 Month-over-Month Change: Oil vs Shipping")
st.caption(
    "When the shipping line (green) rises above the oil line (blue), "
    "stress is building — the gap between them drives the stress signal."
)

fig_mom = go.Figure()
fig_mom.add_trace(go.Scatter(
    x=df['month'], y=df['oil_mom_change_pct'],
    mode='lines+markers',
    name='Oil Price MoM%',
    line=dict(color='#4fc3f7', width=2),
    marker=dict(size=5)
))
fig_mom.add_trace(go.Scatter(
    x=df['month'], y=df['shipping_mom_change_pct'],
    mode='lines+markers',
    name='Shipping MoM%',
    line=dict(color='#81c784', width=2),
    marker=dict(size=5)
))
fig_mom.add_hline(y=0, line_color="#555555", line_width=1)
fig_mom.update_layout(
    plot_bgcolor='#1e2130',
    paper_bgcolor='#1e2130',
    font_color='#cccccc',
    hovermode='x unified',
    legend=dict(orientation='h', yanchor='bottom', y=1.02),
    height=350,
    yaxis_title='MoM Change %',
    xaxis_title=''
)
st.plotly_chart(fig_mom, use_container_width=True)

st.divider()

# ── HOW TO READ ───────────────────────────────────────────────────────────────
with st.expander("📖 How to read this dashboard"):
    st.markdown(f"""
    **What is the Stress Signal?**

    The stress signal is computed as:
    ```
    stress_signal = shipping_MoM_change% − oil_MoM_change%
    ```
    When shipping costs rise faster than oil prices, goods are moving but buffers are thinning —
    historically a leading indicator of supply chain pressure 2–3 weeks ahead.

    **Traffic Light System**
    - 🟢 **Calm** (stress < {STRESS_THRESHOLD}): Normal supply chain conditions
    - 🔴 **Stress** (stress ≥ {STRESS_THRESHOLD}): Shipping outpacing oil — monitor closely

    **Data Sources**
    - **Oil Price**: EIA WTI Crude Oil Spot Price, Cushing Oklahoma (daily, averaged monthly)
    - **Shipping Index**: FRED PPIFIS — Producer Price Index for Transportation & Warehousing (monthly)

    **Limitations**
    - PPIFIS has a 4–6 week publication lag — the signal always trails by one month on the shipping side
    - This is a leading indicator, not a prediction — use alongside other market signals
    - Based on US data — may not fully capture Asia-Pacific or European supply chain dynamics
    """)

# ── RAW DATA ──────────────────────────────────────────────────────────────────
with st.expander("🗃️ Raw Data Table"):
    display_df = df.copy()
    display_df['month'] = display_df['month'].dt.strftime('%Y-%m')
    display_df = display_df.drop(columns=['signal_color'])
    display_df.columns = [
        'Month', 'Avg Oil Price ($/BBL)', 'Shipping Index',
        'Oil MoM %', 'Shipping MoM %', 'Stress Signal'
    ]
    display_df = display_df.sort_values('Month', ascending=False)
    st.dataframe(display_df, use_container_width=True, hide_index=True)

# ── FOOTER ────────────────────────────────────────────────────────────────────
st.divider()
st.markdown("""
<div style="text-align: center; color: #555555; font-size: 0.8em;">
    Built by Jay Kumar Dubey &nbsp;·&nbsp;
    <a href="https://github.com/jay-kumar-dubey/Commodity-Supply-Chain-Pipeline" style="color: #555555;">
    GitHub</a> &nbsp;·&nbsp;
    Data: EIA API + FRED API &nbsp;·&nbsp;
    Pipeline: Python → AWS S3 → dbt → DuckDB → Streamlit
</div>
""", unsafe_allow_html=True)