# Jose Hernandez
# D610 Capstone Data Engineering

# Here are my sources I used to make this script that I made using WGU surces for APA format
"""
- SpaceX-API (n.d.). SpaceX API documentation (v4). Feb 16, 2026, from https://docs.spacexdata.com/
- SpaceX-API (n.d.). SpaceX-API GitHub documentation. Feb 16, 2026, from https://github.com/r-spacex/SpaceX-API
- DuckDB (n.d.). Python DB API. Feb 16, 2026, from https://duckdb.org/docs/stable/clients/python/dbapi.html
- DuckDB (n.d.). CREATE TABLE statement. Feb 16, 2026, from https://duckdb.org/docs/stable/sql/statements/create_table.html
- statsmodels (n.d.). Generalized linear models (GLM).Feb 16, 2026, from https://www.statsmodels.org/stable/glm.html
- statsmodels (n.d.). GLM class reference. Feb 16, 2026, from https://www.statsmodels.org/devel/generated/statsmodels.genmod.generalized_linear_model.GLM.html
- Streamlit (n.d.). st.bar_chart. Feb 16, 2026, from https://docs.streamlit.io/develop/api-reference/charts/st.bar_chart
"""

# This script will run the Streamlit dashboard and read from db/spacex.db and display it visually.
# It should show the success-rate bar chart, odds ratio tbale with a p-value and regression summary

from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import streamlit as st
import statsmodels.formula.api as smf
import statsmodels.api as sm
import statsmodels.formula.api as smf

DB_PATH = Path("db/spacex.duckdb")

@st.cache_data(show_spinner=False)
def load_data() -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    con.execute("SET schema='spacex';")
    df = con.execute("SELECT * FROM v_core_landings_model;").df()
    con.close()

    df = df[df["flight_number"].notna()].copy()
    df = df[df["landing_type"].notna()].copy()

    df["flight_number"] = df["flight_number"].astype(int)
    df["is_reused"] = df["is_reused"].astype(int)
    df["landing_success"] = df["landing_success"].astype(int)
    return df

def fit_model(df: pd.DataFrame):
    model = smf.glm(
        formula="landing_success ~ is_reused + flight_number + C(landing_type)",
        data=df,
        family=sm.families.Binomial()
    )
    return model.fit()

def success_rates(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby("is_reused")["landing_success"].agg(attempts="count", successes="sum").reset_index()
    g["success_rate"] = g["successes"] / g["attempts"]
    g["group"] = g["is_reused"].map({0: "New", 1: "Reused"})
    return g[["group", "attempts", "successes", "success_rate"]]

def safe_exp(x, cap=50):
    # exp(50) ~ 5.2e21, already astronomically large but avoids overflow
    x = np.clip(x, -cap, cap)
    return np.exp(x)

def odds_ratio_table(res) -> pd.DataFrame:
    params = res.params
    conf = res.conf_int()
    pvals = res.pvalues

    return pd.DataFrame({
        "term": params.index,
        "coef_log_odds": params.values,
        "odds_ratio": safe_exp(params.values),
        "p_value": pvals.values,
        "ci_low_or": safe_exp(conf[0].values),
        "ci_high_or": safe_exp(conf[1].values),
    }).sort_values("p_value")

st.set_page_config(page_title="SpaceX Landing Success Regression", layout="wide")
st.title("SpaceX Booster Landing Success: Logistic Regression")

if not DB_PATH.exists():
    st.error("Missing db/spacex.duckdb. Run: python scripts/02_transform_load_duckdb.py")
    st.stop()

df = load_data()

rocket_options = ["All"] + sorted(df["rocket_name"].dropna().unique().tolist())
rocket_choice = st.selectbox("Rocket filter", rocket_options, index=0)
if rocket_choice != "All":
    df = df[df["rocket_name"] == rocket_choice].copy()

c1, c2 = st.columns([1, 1])

with c1:
    st.subheader("Success Rate (New vs Reused)")
    rates = success_rates(df)
    st.dataframe(rates, use_container_width=True)

    chart_df = rates.set_index("group")[["success_rate"]]
    st.bar_chart(chart_df)

with c2:
    st.subheader("Regression (Odds Ratios + p-values)")
    st.caption("Model: landing_success ~ is_reused + flight_number + C(landing_type)")

    res = fit_model(df)
    tbl = odds_ratio_table(res)

    st.dataframe(tbl, use_container_width=True)
    st.text(res.summary().as_text())
