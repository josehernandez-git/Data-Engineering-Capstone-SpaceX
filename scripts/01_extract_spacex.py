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

# this script connects to the Spcae X API and downlaods the JSON data for launches, cores, rockets, payloads, and pads
# then stores it in the data/raw folder for an original copy

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://api.spacexdata.com/v4"

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

ENDPOINTS: Dict[str, str] = {
    "launches": f"{BASE_URL}/launches",
    "cores": f"{BASE_URL}/cores",
    "payloads": f"{BASE_URL}/payloads",
    "rockets": f"{BASE_URL}/rockets",
    "launchpads": f"{BASE_URL}/launchpads",
    "landpads": f"{BASE_URL}/landpads",
}
# This is to make a session for the retries like the 429 and errors
def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session
# this fucntion will get the json file from the api
def fetch_json(session: requests.Session, url: str) -> Any:
    resp = session.get(url, timeout=60)
    resp.raise_for_status()
    return resp.json()
# this will save the json
def save_json(obj: Any, path: Path) -> None:
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")

def main() -> None:
    session = make_session()
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") # make the nice timezone

    for name, url in ENDPOINTS.items():
        print(f"Downloading {name} ...")
        data = fetch_json(session, url)
        save_json(data, RAW_DIR / f"{name}.json") # snapshot1
        save_json(data, RAW_DIR / f"{name}_{ts}.json")     # snapshot 2
        time.sleep(0.2)

    print("Done. Raw files are in data/raw/")

if __name__ == "__main__":
    main()
