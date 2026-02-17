# Jose Hernandez
# D610 Capstone Data Engineering

# Here are my sources I used to make this script that I made using WGU surces for APA format
"""
- SpaceX-API (n.d.). SpaceX API documentation (v4).  Feb 16, 2026, from https://docs.spacexdata.com/
- SpaceX-API (n.d.). SpaceX-API GitHub documentation.  Feb 16, 2026, from https://github.com/r-spacex/SpaceX-API
- DuckDB (n.d.). Python DB API.  Feb 16, 2026, from https://duckdb.org/docs/stable/clients/python/dbapi.html
- DuckDB (n.d.). CREATE TABLE statement.  Feb 16, 2026, from https://duckdb.org/docs/stable/sql/statements/create_table.html
- statsmodels (n.d.). Generalized linear models (GLM).  Feb 16, 2026, from https://www.statsmodels.org/stable/glm.html
- statsmodels (n.d.). GLM class reference.  Feb 16, 2026, from https://www.statsmodels.org/devel/generated/statsmodels.genmod.generalized_linear_model.GLM.html
- Streamlit (n.d.). st.bar_chart.  Feb 16, 2026, from https://docs.streamlit.io/develop/api-reference/charts/st.bar_chart
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
import statsmodels.api as sm
import statsmodels.formula.api as smf

# this script loads the analysis ready view from DuckDB and calculates the new vs reused ladning success rates and
# runs the logistic regression. It also prints the odds ratio, p-value, ci, and model summary
DB_PATH = Path("db/spacex.duckdb")

def load_model_data() -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    con.execute("SET schema='spacex';")
    df = con.execute("SELECT * FROM v_core_landings_model;").df()
    con.close()

    # Ensure predictors exist
    df = df[df["flight_number"].notna()].copy()
    df = df[df["landing_type"].notna()].copy()

    df["flight_number"] = df["flight_number"].astype(int)
    df["is_reused"] = df["is_reused"].astype(int)
    df["landing_success"] = df["landing_success"].astype(int)
    return df

def success_rate_summary(df: pd.DataFrame) -> pd.DataFrame:
    out = df.groupby("is_reused")["landing_success"].agg(attempts="count", successes="sum").reset_index()
    out["success_rate"] = out["successes"] / out["attempts"]
    out["group"] = out["is_reused"].map({0: "New", 1: "Reused"})
    return out[["group", "attempts", "successes", "success_rate"]]

def fit_model(df: pd.DataFrame):
    model = smf.glm(
        formula="landing_success ~ is_reused + flight_number + C(landing_type)",
        data=df,
        family=sm.families.Binomial()
    )
    return model.fit()

def odds_ratio_table(res) -> pd.DataFrame:
    params = res.params
    conf = res.conf_int()
    pvals = res.pvalues

    tbl = pd.DataFrame({
        "term": params.index,
        "coef_log_odds": params.values,
        "odds_ratio": np.exp(params.values),
        "p_value": pvals.values,
        "ci_low_or": np.exp(conf[0].values),
        "ci_high_or": np.exp(conf[1].values),
    }).sort_values("p_value")

    return tbl

def main() -> None:
    df = load_model_data()

    print("=== Success rates (New vs Reused) ===")
    print(success_rate_summary(df).to_string(index=False))

    res = fit_model(df)

    print("\n=== Odds ratios & p-values ===")
    print(odds_ratio_table(res).to_string(index=False))

    print("\n=== Full model summary ===")
    print(res.summary())

if __name__ == "__main__":
    main()
