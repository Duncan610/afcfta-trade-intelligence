import os
import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

st.set_page_config(page_title="AfCFTA Trade Intelligence", layout="wide")
st.title("AfCFTA Trade Intelligence — Milestone 1")
st.caption("Compare applied vs. MFN tariff treatment and trade volume across 5 African economies")

WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
cfg = Config()  # picks up the app's own identity inside Databricks Apps
HOST = (cfg.host or "").replace("https://", "").replace("http://", "").rstrip("/")

with st.expander("Connection details (for troubleshooting)"):
    st.write("Workspace host:", HOST or "NOT FOUND")
    st.write("Warehouse ID:", WAREHOUSE_ID or "NOT FOUND")

NUMERIC_COLS = [
    "year", "total_trade_value_usd", "applied_tariff_pct",
    "mfn_tariff_pct", "preferential_discount_pct", "tariff_year",
]


@st.cache_data(ttl=600, show_spinner="Loading data from the gold table...")
def load_data(host: str, warehouse_id: str) -> pd.DataFrame:
    with sql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        credentials_provider=lambda: cfg.authenticate,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM afcfta_trade.gold.market_opportunity")
            df = cur.fetchall_arrow().to_pandas()
    # Decimal columns arrive as Python objects; convert so charts and sums work
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


try:
    df = load_data(HOST, WAREHOUSE_ID)
except Exception as e:
    st.error(f"Could not load data: {type(e).__name__}: {e}")
    if e.__cause__ is not None:
        st.code(repr(e.__cause__))
    st.info(
        "Most likely causes: the app's service principal lacks 'Can use' on the "
        "SQL warehouse, the warehouse ID is wrong, or the SELECT/USE grants are missing."
    )
    st.stop()

chapters = df[["hs_chapter", "product_name"]].drop_duplicates().sort_values("hs_chapter")
names = dict(zip(chapters["hs_chapter"], chapters["product_name"]))
chapter_choice = st.selectbox(
    "Select an HS chapter",
    options=list(names.keys()),
    format_func=lambda c: f"HS {c} — {names[c]}",
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
    st.caption("Egypt has no applied/MFN tariff data. See the README for why.")

with col2:
    st.subheader("Trade value by country (2023, US$)")
    trade_2023 = (
        filtered[filtered["year"] == 2023]
        .pivot_table(index="reporter_iso3", columns="flow_code",
                     values="total_trade_value_usd", aggfunc="sum")
        .rename(columns={"M": "Imports", "X": "Exports"})
    )
    st.bar_chart(trade_2023)

st.divider()
st.subheader("Full detail")
st.dataframe(filtered, use_container_width=True)
