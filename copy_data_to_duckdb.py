import duckdb
from config import DUCKDB_FILE, ZONAL_STATS_DIR, INSEE_TILE_FILE

def copy_data_to_duckdb():
    copy_insee_tile_to_duckdb()
    copy_zonal_stats_to_duckdb()

def copy_zonal_stats_to_duckdb():
    con = duckdb.connect(DUCKDB_FILE)
    q = f"""
    CREATE OR REPLACE TABLE zonal_stats AS
    SELECT * 
    FROM read_parquet('{ZONAL_STATS_DIR}/*.parquet')
    """
    con.execute(q)
    con.close()

def copy_insee_tile_to_duckdb():
    con = duckdb.connect(DUCKDB_FILE)
    q = f"""
    INSTALL spatial;
    LOAD spatial;

    CREATE OR REPLACE TABLE insee_tile AS
    SELECT * 
    FROM read_parquet('{INSEE_TILE_FILE}')
    """
    con.execute(q)
    con.close()