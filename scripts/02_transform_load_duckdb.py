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

# This script will read the downloaded JSOn files and clean/flattens the nested structure. Then build a relational tables
# for modeling and then load into the DuckDB database file
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import duckdb
import pandas as pd

RAW_DIR = Path("data/raw")
DB_PATH = Path("db/spacex.duckdb")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))

def normalize_dim(df: pd.DataFrame, id_col: str = "id") -> pd.DataFrame:
    if id_col in df.columns:
        return df.drop_duplicates(subset=[id_col]).reset_index(drop=True)
    return df.reset_index(drop=True)

def build_fact_core_landings(launches: List[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for L in launches:
        for c in (L.get("cores") or []):
            rows.append({
                "launch_id": L.get("id"),
                "launch_name": L.get("name"),
                "date_utc": L.get("date_utc"),
                "rocket_id": L.get("rocket"),
                "launchpad_id": L.get("launchpad"),
                "launch_success_flag": L.get("success"),

                "core_id": c.get("core"),
                "flight_number": c.get("flight"),
                "is_reused": c.get("reused"),
                "landing_attempt": c.get("landing_attempt"),
                "landing_success": c.get("landing_success"),
                "landing_type": c.get("landing_type"),
                "landpad_id": c.get("landpad"),
            })

    df = pd.DataFrame(rows)

    # Keep only attempted landings with known outcomes and known reuse status
    df = df[df["landing_attempt"] == True].copy()
    df = df[df["landing_success"].notna()].copy()
    df = df[df["is_reused"].notna()].copy()

    # Convert to 0/1 for modeling
    df["landing_success"] = df["landing_success"].astype(int)
    df["is_reused"] = df["is_reused"].astype(int)

    return df.reset_index(drop=True)

def main() -> None:
    launches = load_json(RAW_DIR / "launches.json")
    cores = load_json(RAW_DIR / "cores.json")
    rockets = load_json(RAW_DIR / "rockets.json")
    payloads = load_json(RAW_DIR / "payloads.json")
    launchpads = load_json(RAW_DIR / "launchpads.json")
    landpads = load_json(RAW_DIR / "landpads.json")

    dim_core = normalize_dim(pd.json_normalize(cores), "id")[["id", "serial", "block", "reuse_count", "status"]].copy()
    dim_rocket = normalize_dim(pd.json_normalize(rockets), "id")[["id", "name", "type", "active", "stages", "boosters"]].copy()
    dim_payload = normalize_dim(pd.json_normalize(payloads), "id")[["id", "name", "type", "mass_kg", "orbit"]].copy()
    dim_launchpad = normalize_dim(pd.json_normalize(launchpads), "id")[["id", "name", "full_name", "region", "locality", "latitude", "longitude"]].copy()
    dim_landpad = normalize_dim(pd.json_normalize(landpads), "id")[["id", "name", "type", "locality", "region", "latitude", "longitude"]].copy()

    fact_core_landings = build_fact_core_landings(launches)

    con = duckdb.connect(str(DB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS spacex;")
    con.execute("SET schema='spacex';")

    # this where I replace the tables for repeatability
    con.register("dim_core_df", dim_core)
    con.register("dim_rocket_df", dim_rocket)
    con.register("dim_payload_df", dim_payload)
    con.register("dim_launchpad_df", dim_launchpad)
    con.register("dim_landpad_df", dim_landpad)
    con.register("fact_core_landings_df", fact_core_landings)

    con.execute("CREATE OR REPLACE TABLE dim_core AS SELECT * FROM dim_core_df;")
    con.execute("CREATE OR REPLACE TABLE dim_rocket AS SELECT * FROM dim_rocket_df;")
    con.execute("CREATE OR REPLACE TABLE dim_payload AS SELECT * FROM dim_payload_df;")
    con.execute("CREATE OR REPLACE TABLE dim_launchpad AS SELECT * FROM dim_launchpad_df;")
    con.execute("CREATE OR REPLACE TABLE dim_landpad AS SELECT * FROM dim_landpad_df;")
    con.execute("CREATE OR REPLACE TABLE fact_core_landings AS SELECT * FROM fact_core_landings_df;")

    con.execute("""
        CREATE OR REPLACE VIEW v_core_landings_model AS
        SELECT
            f.*,
            r.name AS rocket_name,
            lp.name AS launchpad_name,
            ldp.type AS landpad_type
        FROM fact_core_landings f
        LEFT JOIN dim_rocket r ON r.id = f.rocket_id
        LEFT JOIN dim_launchpad lp ON lp.id = f.launchpad_id
        LEFT JOIN dim_landpad ldp ON ldp.id = f.landpad_id;
    """)

    n = con.execute("SELECT COUNT(*) FROM fact_core_landings;").fetchone()[0]
    print(f"Created/updated DB: {DB_PATH}")
    print(f"Rows in spacex.fact_core_landings: {n}")
    con.close()

if __name__ == "__main__":
    main()
