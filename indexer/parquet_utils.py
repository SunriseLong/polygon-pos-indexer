import os
import logging
import pyarrow as pa
import pyarrow.parquet as pq
import shutil
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def write_events_to_parquet(filepath: str, events: List[Dict[str, Any]], schema: pa.Schema) -> bool:
    if events:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        logging.info(f"  Writing {len(events)} events to parquet file (TEMP): {filepath}")
        table = pa.Table.from_pylist(events, schema=schema)
        pq.write_table(table, filepath)
        logging.info(f"  Parquet file written to TEMP directory: {filepath}")
        return True
    return False

def generate_parquet_filepath(base_dir: str, start_block: int, end_block: int) -> str:
    """Generates a Parquet filepath based on block range."""
    return os.path.join(base_dir, f"{start_block}_{end_block}.parquet")

def setup_temporary_directory(base_dir: str, address: str, event_name: str) -> str:
    temp_dir = os.path.join(base_dir, f"temp_{event_name}", address)
    os.makedirs(temp_dir, exist_ok=True)
    logging.info(f"Temporary parquet directory created: {temp_dir}")
    return temp_dir

def atomic_directory_replace(temp_dir: str, final_dir: str) -> bool:
    """Atomically replaces the final directory with the temporary directory."""
    try:
        logging.info(f"Replacing original parquet directory '{final_dir}' with temporary directory '{temp_dir}'...")
        os.makedirs(os.path.dirname(final_dir), exist_ok=True)
        
        if os.path.exists(final_dir):
            shutil.rmtree(final_dir)
            logging.info(f"Existing parquet directory '{final_dir}' removed.")
        os.rename(temp_dir, final_dir)
        logging.info(f"Parquet directory atomically replaced. New directory is: '{final_dir}'")
        return True
    except OSError as e:
        logging.error(f"Error replacing parquet directory: {e}")
        logging.error(f"Parquet files are still in temporary directory: '{temp_dir}'")
        return False