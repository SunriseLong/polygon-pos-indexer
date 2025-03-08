import duckdb
import os
import logging

def update_duckdb_from_parquet(parquet_base_dir, duckdb_database_file="polygon_pos.duckdb"):
    """
    Updates a DuckDB database table from Parquet files in a given directory. Persists DB file after application is closed.

    Args:
        parquet_base_dir (str): Base directory containing Parquet files.
        duckdb_database_file (str, optional): Path to the DuckDB database file. Defaults to "delegator_rewards.duckdb".
    """
    con = duckdb.connect(database=duckdb_database_file)
    logging.info(f"Connected to DuckDB database: {duckdb_database_file}")

    parquet_glob_pattern = os.path.join(parquet_base_dir, "**", "*.parquet")
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE delegator_claimed_rewards AS
            SELECT * FROM read_parquet('{parquet_glob_pattern}')
        """)
        logging.info(f"DuckDB table delegator_claimed_rewards created/updated from parquet files in: {parquet_base_dir}")
    except Exception as e:
        logging.error(f"Error loading parquet files into DuckDB: {e}")
    finally:
        con.close()
        logging.info("DuckDB connection closed.")