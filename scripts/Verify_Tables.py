import duckdb
con = duckdb.connect("db/spacex.duckdb", read_only=True)
con.execute("SET schema='spacex';")
print(con.execute("SHOW TABLES").fetchall())
print(con.execute("SELECT COUNT(*) FROM fact_core_landings").fetchone())
con.close()
# this script is to make sure my tables are correct from the DB