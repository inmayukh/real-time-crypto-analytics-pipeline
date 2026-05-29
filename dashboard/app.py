import os
import time

import clickhouse_connect
import pandas as pd
import streamlit as st


CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "crypto_analytics")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "admin")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "admin123")


st.set_page_config(
    page_title="Real-Time Crypto Analytics",
    layout="wide",
)


@st.cache_resource
def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )


def query_df(sql):
    client = get_clickhouse_client()
    result = client.query(sql)
    return pd.DataFrame(result.result_rows, columns=result.column_names)


st.title("Real-Time Crypto Market Analytics Dashboard")

st.caption(
    "Live Binance WebSocket data streamed through Kafka, stored in ClickHouse, transformed with dbt, and monitored with Airflow."
)

refresh_seconds = st.sidebar.slider(
    "Auto-refresh interval seconds",
    min_value=5,
    max_value=60,
    value=10,
    step=5,
)

st.sidebar.info(f"Dashboard refreshes every {refresh_seconds} seconds.")


latest_prices = query_df("""
    SELECT
        symbol,
        base_asset,
        price,
        open_price,
        high_price,
        low_price,
        volume,
        quote_volume,
        event_timestamp,
        ingestion_timestamp
    FROM crypto_analytics.latest_crypto_ticker_prices
    ORDER BY quote_volume DESC
    LIMIT 50
""")


pipeline_health = query_df("""
    SELECT *
    FROM crypto_analytics.pipeline_health_summary
""")


raw_stats = query_df("""
    SELECT
        count() AS total_raw_events,
        countDistinct(symbol) AS unique_symbols,
        max(ingestion_timestamp) AS latest_ingestion_timestamp
    FROM crypto_analytics.raw_crypto_ticker_events
""")


recent_stats = query_df("""
    SELECT
        count() AS events_last_5_min,
        countDistinct(symbol) AS symbols_last_5_min
    FROM crypto_analytics.raw_crypto_ticker_events
    WHERE ingestion_timestamp >= now() - INTERVAL 5 MINUTE
""")


failed_stats = query_df("""
    SELECT
        count() AS failed_events_last_15_min
    FROM crypto_analytics.failed_crypto_events
    WHERE failed_at >= now() - INTERVAL 15 MINUTE
""")


col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Total Raw Events",
    int(raw_stats["total_raw_events"].iloc[0]) if not raw_stats.empty else 0,
)

col2.metric(
    "Unique Symbols",
    int(raw_stats["unique_symbols"].iloc[0]) if not raw_stats.empty else 0,
)

col3.metric(
    "Events Last 5 Min",
    int(recent_stats["events_last_5_min"].iloc[0]) if not recent_stats.empty else 0,
)

col4.metric(
    "Failed Events Last 15 Min",
    int(failed_stats["failed_events_last_15_min"].iloc[0]) if not failed_stats.empty else 0,
)


st.divider()

st.subheader("Latest Prices")

st.dataframe(
    latest_prices,
    use_container_width=True,
    hide_index=True,
)


st.subheader("Top Coins by Quote Volume")

if not latest_prices.empty:
    volume_chart = latest_prices[["symbol", "quote_volume"]].head(20)
    st.bar_chart(volume_chart.set_index("symbol"))


st.subheader("Rolling Price Trend")

selected_symbol = st.selectbox(
    "Select symbol",
    options=latest_prices["symbol"].tolist() if not latest_prices.empty else [],
)

if selected_symbol:
    price_trend = query_df(f"""
        SELECT
            toStartOfMinute(event_timestamp) AS minute,
            avg(price) AS avg_price
        FROM crypto_analytics.stg_crypto_ticker_events
        WHERE symbol = '{selected_symbol}'
          AND event_timestamp >= now() - INTERVAL 60 MINUTE
        GROUP BY minute
        ORDER BY minute
    """)

    if not price_trend.empty:
        price_trend["minute"] = pd.to_datetime(price_trend["minute"])
        st.line_chart(price_trend.set_index("minute")["avg_price"])
    else:
        st.warning("No recent price trend data found for selected symbol.")


st.subheader("Pipeline Health Summary")

st.dataframe(
    pipeline_health,
    use_container_width=True,
    hide_index=True,
)


time.sleep(refresh_seconds)
st.rerun()