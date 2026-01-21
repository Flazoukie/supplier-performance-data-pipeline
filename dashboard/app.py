"""This script implements a Streamlit dashboard to visualize supplier performance and risk data
from a DuckDB database.
It includes filters, KPI tiles, ranking tables, charts, and a drill-down section for individual suppliers.

First part: dashboard with filters, KPIs, tables, and charts.
Second part: table viewer for all DuckDB tables.
"""

# library imports
from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st


# ---------- App config ----------
# page configuration for Streamlit (wide layout, titles + explanation at the top)
st.set_page_config(
    page_title="Supplier Performance & Risk",
    layout="wide",
)

st.title("Supplier Performance & Risk Dashboard")
st.caption(
    "Synthetic procurement pipeline → DuckDB → KPIs → Risk score. "
    "This dashboard reads from DuckDB tables: supplier_kpis and supplier_risk_summary."
)

# ---------- Paths ----------
PROJECT_ROOT = Path(__file__).resolve().parents[1] # gets the root of the project
DB_PATH = PROJECT_ROOT / "warehouse.db" # path to the DuckDB database file


@st.cache_data(show_spinner=False) # caches the loaded data to avoid reloading on every interaction
def load_supplier_risk_summary(db_path: Path) -> pd.DataFrame: # loads supplier risk summary data from DuckDB
    con = duckdb.connect(str(db_path), read_only=True) # opens a read-only connection to the DuckDB database
    try: # loads the supplier risk summary data from the database
        df = con.execute(
            """
            SELECT
                supplier_id,
                supplier_name,
                category,
                country,
                financial_risk_score,
                n_pos,
                on_time_delivery_rate,
                avg_delivery_delay_days,
                fill_rate,
                quality_issue_rate,
                performance_score,
                risk_score
            FROM supplier_risk_summary
            ORDER BY risk_score DESC;
            """
        ).fetchdf() # fetches the result as a pandas DataFrame
    finally: # ensures the connection is closed after data is fetched
        con.close()

    # Pretty formatting helpers (keep raw values too)
    df["on_time_delivery_rate_pct"] = (df["on_time_delivery_rate"] * 100).round(1)
    df["fill_rate_pct"] = (df["fill_rate"] * 100).round(1)
    df["quality_issue_rate_pct"] = (df["quality_issue_rate"] * 100).round(1)
    df["avg_delivery_delay_days"] = df["avg_delivery_delay_days"].round(2)
    df["performance_score"] = df["performance_score"].round(3)
    df["risk_score"] = df["risk_score"].round(3)
    return df


# ---------- Load data ----------
# Check if the DuckDB file exists
if not DB_PATH.exists():
    st.error(f"Could not find DuckDB file at: {DB_PATH}\n\nRun src/load_duckdb.py first.")
    st.stop()

# Load supplier risk summary data
df = load_supplier_risk_summary(DB_PATH)

# ---------- Sidebar filters ----------
# Sidebar filters for category, country, number of POs, and top N suppliers
st.sidebar.header("Filters")

categories = ["All"] + sorted(df["category"].dropna().unique().tolist())
countries = ["All"] + sorted(df["country"].dropna().unique().tolist())

cat = st.sidebar.selectbox("Category", categories, index=0)
cty = st.sidebar.selectbox("Country", countries, index=0)

min_pos = int(df["n_pos"].min())
max_pos = int(df["n_pos"].max())
pos_range = st.sidebar.slider("Number of POs (n_pos)", min_pos, max_pos, (min_pos, max_pos))

top_n = st.sidebar.slider("Top N suppliers to show in charts", 5, 15, 10)

filtered = df.copy()
if cat != "All":
    filtered = filtered[filtered["category"] == cat]
if cty != "All":
    filtered = filtered[filtered["country"] == cty]
filtered = filtered[(filtered["n_pos"] >= pos_range[0]) & (filtered["n_pos"] <= pos_range[1])]

# ---------- KPI tiles ----------
# Display KPI tiles for number of suppliers, average risk score, average on-time rate, and average fill rate
col1, col2, col3, col4 = st.columns(4)
col1.metric("Suppliers (filtered)", f"{len(filtered)}")
col2.metric("Avg Risk Score", f"{filtered['risk_score'].mean():.3f}" if len(filtered) else "—")
col3.metric("Avg On-Time Rate", f"{filtered['on_time_delivery_rate'].mean()*100:.1f}%" if len(filtered) else "—")
col4.metric("Avg Fill Rate", f"{filtered['fill_rate'].mean()*100:.1f}%" if len(filtered) else "—")

st.divider() # divider between sections

# ---------- Ranking table ----------
# Display a table of suppliers with KPIs and risk scores
st.subheader("Supplier table (KPIs + Risk Score)")
display_cols = [
    "supplier_id",
    "supplier_name",
    "category",
    "country",
    "financial_risk_score",
    "n_pos",
    "on_time_delivery_rate_pct",
    "avg_delivery_delay_days",
    "fill_rate_pct",
    "quality_issue_rate_pct",
    "performance_score",
    "risk_score",
]

st.dataframe(
    filtered[display_cols],
    use_container_width=True,
    hide_index=True,
)

st.caption(
    "Rates shown as percentages. Delay is in days (negative means early deliveries on average)."
)

st.divider()

# ---------- Charts ----------
# Display bar charts for on-time delivery rate and average delivery delay for top N risky suppliers
st.subheader("Charts")

top = filtered.sort_values("risk_score", ascending=False).head(top_n)

c1, c2 = st.columns(2)

# On-Time Delivery Rate Chart
with c1:
    st.markdown("**On-Time Delivery Rate (Top by Risk)**")
    chart_df = top[["supplier_name", "on_time_delivery_rate"]].set_index("supplier_name")
    st.bar_chart(chart_df)

# Average Delivery Delay Chart
with c2:
    st.markdown("**Average Delivery Delay in Days (Top by Risk)**")
    chart_df = top[["supplier_name", "avg_delivery_delay_days"]].set_index("supplier_name")
    st.bar_chart(chart_df)

st.divider()

# ---------- Quick drill-down ----------
# Drill-down section to select a supplier and view detailed KPIs and risk scores
st.subheader("Drill-down: select one supplier")
supplier_names = filtered["supplier_name"].tolist()
if supplier_names:
    selected = st.selectbox("Supplier", supplier_names, index=0)
    row = filtered[filtered["supplier_name"] == selected].iloc[0]

    st.write(
        {
            "supplier_id": row["supplier_id"],
            "category": row["category"],
            "country": row["country"],
            "n_pos": int(row["n_pos"]),
            "financial_risk_score": int(row["financial_risk_score"]),
            "on_time_delivery_rate": float(row["on_time_delivery_rate"]),
            "avg_delivery_delay_days": float(row["avg_delivery_delay_days"]),
            "fill_rate": float(row["fill_rate"]),
            "quality_issue_rate": float(row["quality_issue_rate"]),
            "performance_score": float(row["performance_score"]),
            "risk_score": float(row["risk_score"]),
        }
    )
else:
    st.info("No suppliers match the current filters.")


# visualize tables in Streamlit
# ---------- Database table viewer ----------

st.divider()
st.subheader("Database table viewer")

@st.cache_data(show_spinner=False)
def load_table(db_path: Path, table_name: str, limit: int) -> pd.DataFrame:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        return con.execute(f"SELECT * FROM {table_name} LIMIT {limit};").fetchdf()
    finally:
        con.close()

tables = ["suppliers", "purchase_orders", "deliveries", "supplier_kpis", "supplier_risk_summary"]
table = st.selectbox("Choose a table", tables, index=tables.index("supplier_risk_summary"))
limit = st.slider("Rows to display", 10, 500, 50, step=10)

st.dataframe(load_table(DB_PATH, table, limit), use_container_width=True)

st.caption("Table data loaded from DuckDB.")
