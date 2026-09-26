import streamlit as st
from databricks import sql
from databricks.sdk.core import Config
import pandas as pd
import os

st.set_page_config(page_title="AfCFTA Trade Intelligence", layout="wide")
st.title("AfCFTA Trade Intelligence — Milestone 1")
st.caption("Compare applied vs. MFN tariff treatment and trade volume across 5 African economies")

cfg = Config()  # auto-authenticates using the app's own identity in Databricks


@st.cache_data(ttl=600)
def load_data():
    conn = sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate,
    )
    query = "SELECT * FROM afcfta_trade.gold.market_opportunity"
    df = pd.read_sql(query, conn)
    conn.close()
    return df


df = load_data()

chapters = df[["hs_chapter", "product_name"]].drop_duplicates().sort_values("hs_chapter")
chapter_choice = st.selectbox(
    "Select an HS chapter",
    options=chapters["hs_chapter"],
    format_func=lambda c: f"HS {c} — {chapters[chapters.hs_chapter == c]['product_name'].iloc[0]}",
)

filtered = df[df["hs_chapter"] == chapter_choice]

col1, col2 = st.columns(2)

with col1:
    st.subheader("Tariff comparison by country")
    tariff_view = (
        filtered[["reporter_iso3", "applied_tariff_pct", "mfn_tariff_pct", "preferential_discount_pct"]]
        .drop_duplicates()
        .set_index("reporter_iso3")
    )
    st.dataframe(tariff_view, use_container_width=True)
    st.caption("Egypt has no applied/MFN tariff data — see README for why.")

with col2:
    st.subheader("Trade value by country (2023)")
    trade_view = (
        filtered[filtered["year"] == 2023]
        .groupby("reporter_iso3")["total_trade_value_usd"]
        .sum()
    )
    st.bar_chart(trade_view)

st.divider()
st.subheader("Full detail")
st.dataframe(filtered, use_container_width=True)
