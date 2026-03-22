import streamlit as st
import pandas as pd
import plotly.graph_objects as fgo
from plotly.subplots import make_subplots
import os

st.set_page_config(layout="wide", page_title="Stock Analytics Dashboard")

# --- CONFIG ---
PROCESSED_DATA_DIR = "/app/data/processed"

def load_tickers():
    """Find all processed tickers in the gold layer."""
    if not os.path.exists(PROCESSED_DATA_DIR):
        return []
    files = [f for f in os.listdir(PROCESSED_DATA_DIR) if f.endswith("_analytics.parquet")]
    return [f.replace("_analytics.parquet", "") for f in files]

# --- UI SIDEBAR ---
st.sidebar.title("📈 Stock Explorer")
available_tickers = load_tickers()

if not available_tickers:
    st.error("No processed data found. Run your Airflow DAG first!")
    st.stop()

selected_ticker = st.sidebar.selectbox("Select a Ticker", available_tickers)

# Add a Clear Cache button to help with debugging data changes
if st.sidebar.button("Refresh / Clear Cache"):
    st.cache_data.clear()
    st.rerun()

# --- DATA LOADING ---
@st.cache_data
def get_data(ticker):
    path = os.path.join(PROCESSED_DATA_DIR, f"{ticker}_analytics.parquet")
    df = pd.read_parquet(path)
    
    # 1. Spark saves 'date' as a column. Convert and set as index.
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
    
    # 2. Safety: Ensure all column names are lowercase
    df.columns = [c.lower() for c in df.columns]
    df.sort_index(inplace=True)
    return df

df = get_data(selected_ticker)

# --- MAIN DASHBOARD ---
st.title(f"Analytics for {selected_ticker}")

# 1. Metric Row - Using lowercase keys from Spark
last_price = df['close'].iloc[-1]
prev_price = df['close'].iloc[-2]
delta = ((last_price - prev_price) / prev_price) * 100

col1, col2, col3, col4 = st.columns(4)
col1.metric("Current Price", f"${last_price:.2f}", f"{delta:.2f}%")

# Using .get() or checking existence to prevent crashes if Spark job is still running
rsi_val = df['rsi_14'].iloc[-1] if 'rsi_14' in df.columns else 0.0
vol_val = df['volatility_30d'].iloc[-1] if 'volatility_30d' in df.columns else 0.0
sma_50_val = df['sma_50'].iloc[-1] if 'sma_50' in df.columns else 0.0

col2.metric("RSI (14)", f"{rsi_val:.2f}")
col3.metric("Volatility (30d)", f"{vol_val:.2%}")
col4.metric("SMA 50", f"${sma_50_val:.2f}")

# 2. Interactive Charts
fig = make_subplots(
    rows=2, cols=1, 
    shared_xaxes=True, 
    vertical_spacing=0.1, 
    subplot_titles=('Price & Moving Averages', 'RSI Indicator'),
    row_width=[0.3, 0.7]
)

# Candlestick (Lowercase keys)
fig.add_trace(fgo.Candlestick(
    x=df.index, 
    open=df['open'], 
    high=df['high'],
    low=df['low'], 
    close=df['close'], 
    name="Price"
), row=1, col=1)

# Moving Averages (Lowercase keys)
if 'sma_20' in df.columns:
    fig.add_trace(fgo.Scatter(x=df.index, y=df['sma_20'], name="SMA 20", line=dict(color='orange', width=1)), row=1, col=1)
if 'sma_50' in df.columns:
    fig.add_trace(fgo.Scatter(x=df.index, y=df['sma_50'], name="SMA 50", line=dict(color='blue', width=1)), row=1, col=1)
if 'sma_200' in df.columns:
    fig.add_trace(fgo.Scatter(x=df.index, y=df['sma_200'], name="SMA 200", line=dict(color='red', width=1)), row=1, col=1)

# RSI (Lowercase keys)
if 'rsi_14' in df.columns:
    fig.add_trace(fgo.Scatter(x=df.index, y=df['rsi_14'], name="RSI", line=dict(color='purple')), row=2, col=1)
    fig.add_hline(y=70, line_dash="dash", line_color="red", row=2, col=1)
    fig.add_hline(y=30, line_dash="dash", line_color="green", row=2, col=1)

fig.update_layout(
    height=800, 
    template="plotly_dark", 
    showlegend=True, 
    xaxis_rangeslider_visible=False
)
st.plotly_chart(fig, use_container_width=True)

# 3. Raw Data Preview
with st.expander("View Raw Analytics Table"):
    st.dataframe(df.tail(100), use_container_width=True)
