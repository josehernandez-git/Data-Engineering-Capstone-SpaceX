# SpaceX ETL + Landing Success Logistic Regression (DuckDB + Streamlit)

My project builds an end-to-end data engineering pipeline using public SpaceX API v4 data.  
It extracts raw JSON, transforms nested launch/core records into a relational model in DuckDB, runs a logistic regression to estimate landing success drivers, and presents results in a Streamlit dashboard.

---

## What this project does

1. **Extract**: Pull SpaceX rocket/launch/core/pad/payload data from the SpaceX API and save raw JSON snapshots.
2. **Transform**: Flatten nested JSON (especially the `cores[]` array in launches), clean/filter records, and build analysis-ready tables.
3. **Load**: Store tables + a modeling view in a local database file: `db/spacex.duckdb`.
4. **Analyze**: Calculate success rates (new vs reused boosters) and fit a **GLM logistic regression**.
5. **Present**: Streamlit dashboard with success-rate chart, odds ratios, p-values, and model summary.

---

## How to run the pipeline

1) Clone or download all files in my repo
   
3) Install all dependencies:
   
   pip install -r requirements.txt

4) Extract SpaceX API data:

   python scripts/01_extract_spacex.py

5) Transform and Load into the DuckDB:

   python scripts/02_transform_load_duckdb.py

6) Run the analysis:

   python scripts/03_regression_analysis.py

7) Launch the Dashboard:

   python -m streamlit run app.py


<img width="540" height="300" alt="image" src="https://github.com/user-attachments/assets/b969076c-7aef-4018-a677-88aa2404f373" />
