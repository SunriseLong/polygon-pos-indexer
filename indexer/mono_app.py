import os
import logging
import shutil
from web3 import Web3
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb
from environment import PROVIDER_URL, STAKING_INFO_CONTRACT_ADDRESS, TARGET_ADDRESS

# ABI for DelegatorClaimedRewards event from StakingInfo
STAKING_INFO_ABI = [
    {
        "anonymous": False,
        "inputs": [
            {
                "indexed": True,
                "internalType": "uint256",
                "name": "validatorId",
                "type": "uint256",
            },
            {
                "indexed": True,
                "internalType": "address",
                "name": "user",
                "type": "address",
            },
            {
                "indexed": True,
                "internalType": "uint256",
                "name": "rewards",
                "type": "uint256",
            },
        ],
        "name": "DelegatorClaimedRewards",
        "type": "event",
    }
]

# Data validation specification for DelegatorClaimedRewards event
DELEGATOR_CLAIMED_REWARDS_SPEC = {
    "name": "DelegatorClaimedRewards",
    "fields": [
        {"name": "validatorId", "type": int},
        {"name": "user", "type": str},
        {"name": "rewards", "type": int},
    ],
    "root_fields": [
        {"name": "blockNumber", "type": int},
        {"name": "transactionHash", "type": bytes}
    ]
}

# PyArrow schema for the output Parquet files
REWARD_CLAIM_SCHEMA = pa.schema(
    [
        pa.field('block_number', pa.uint64(), nullable=False),
        pa.field('transaction_hash', pa.string(), nullable=False),
        pa.field('validator_id', pa.uint64(), nullable=False),
        pa.field('user_address', pa.string(), nullable=False),
        pa.field('reward_amount_matic', pa.float64(), nullable=False)
    ]
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def validate_data_against_spec(data, spec):
    """
    Validates data against a given specification.

    Args:
        data (dict): Data to validate.
        spec (dict): Specification defining expected fields and types.

    Returns:
        bool: True if data is valid, False otherwise.
    """
    if not isinstance(data, dict):
        raise ValueError("Data is not a dictionary")

    if 'args' not in data or not isinstance(data['args'], dict):
        raise ValueError("Data 'args' field is missing or not a dictionary")

    args = data['args']

    for field_spec in spec.get("fields", []):
        field_name = field_spec["name"]
        field_type = field_spec["type"]
        if field_name not in args:
            raise ValueError(f"Missing field in event args: {field_name}")
        if not isinstance(args[field_name], field_type):
            raise TypeError(f"Field '{field_name}' should be of type {field_type}, but got {type(args[field_name])}")

    for root_field_spec in spec.get("root_fields", []):
        field_name = root_field_spec["name"]
        field_type = root_field_spec["type"]
        if field_name not in data:
            raise ValueError(f"Missing root field in event: {field_name}")
        if not isinstance(data[field_name], field_type):
            raise TypeError(f"Root field '{field_name}' should be of type {field_type}, but got {type(data[field_name])}")

    return True


def fetch_reward_claim_history(address, start_block=19000000, block_increment=1000000):
    """
    Fetches and stores reward claim history, using a temporary directory for atomic replacement.
    """
    logging.info(f"Starting fetch_reward_claim_history for address: {address}, start_block: {start_block}, block_increment: {block_increment}")

    w3 = Web3(Web3.HTTPProvider(PROVIDER_URL))
    if not w3.is_connected():
        raise ConnectionError(f"Failed to connect to Ethereum provider: {PROVIDER_URL}")
    logging.info(f"Connected to Ethereum provider: {PROVIDER_URL}")

    staking_info_contract = w3.eth.contract(address=STAKING_INFO_CONTRACT_ADDRESS, abi=STAKING_INFO_ABI)
    logging.info(f"Using StakingInfo contract at: {STAKING_INFO_CONTRACT_ADDRESS}")

    delegator_claimed_rewards_event = staking_info_contract.events.DelegatorClaimedRewards
    target_checksum_address = w3.to_checksum_address(address)
    logging.info(f"Filtering events for checksum address: {target_checksum_address}")

    parquet_base_dir = "delegator_claimed_rewards"
    wallet_dir = os.path.join(parquet_base_dir, address)
    temp_parquet_dir = os.path.join(parquet_base_dir, f"temp_{address}")

    os.makedirs(temp_parquet_dir, exist_ok=True)
    logging.info(f"Temporary parquet directory created: {temp_parquet_dir}")


    latest_block = w3.eth.block_number
    logging.info(f"Latest block number: {latest_block}")

    current_block_start = start_block

    while current_block_start <= latest_block:
        current_block_end = min(current_block_start + block_increment, latest_block + 1)
        if current_block_start >= current_block_end:
            logging.info(f"current_block_start ({current_block_start}) >= current_block_end ({current_block_end}), breaking loop.")
            break

        to_block = current_block_end - 1
        logging.info(f"--- Processing Block Range: {current_block_start}-{to_block} ---")

        filepath = os.path.join(temp_parquet_dir, f"rewards_{address}_{current_block_start}_{to_block}.parquet") # Save to temp dir
        logging.info(f"  Filepath (TEMP) for this range: {filepath}")


        event_filter = delegator_claimed_rewards_event.create_filter(
            from_block=current_block_start,
            to_block=to_block,
            argument_filters={'user': target_checksum_address}
        )
        events = event_filter.get_all_entries()

        if events:
            logging.info(f"  Found {len(events)} events in block range {current_block_start}-{to_block}")
            processed_events = []
            for event in events:
                try:
                    event_dict = dict(event)
                    event_dict['args'] = dict(event_dict['args'])
                    validate_data_against_spec(event_dict, DELEGATOR_CLAIMED_REWARDS_SPEC)
                    log = event_dict['args']
                    processed_event = {
                        'block_number': event_dict['blockNumber'],
                        'transaction_hash': event_dict['transactionHash'].hex(),
                        'validator_id': log['validatorId'],
                        'user_address': log['user'],
                        'reward_amount_matic': float(w3.from_wei(log['rewards'], 'ether'))
                    }
                    processed_events.append(processed_event)
                except (ValueError, TypeError) as e:
                    logging.error(f"  Data validation error for event in block {event_dict.get('blockNumber', 'Unknown')}: {e}. Event: {event}")
                    continue

            if processed_events:
                logging.info(f"  Writing {len(processed_events)} events to parquet file (TEMP): {filepath}")
                table = pa.Table.from_pylist(processed_events, schema=REWARD_CLAIM_SCHEMA)
                pq.write_table(table, filepath)
                logging.info(f"  Parquet file written to TEMP directory: {filepath}")


        else:
            logging.info(f"  No events found in block range {current_block_start}-{to_block}")


        current_block_start += block_increment

    logging.info("Fetching and parquet processing complete in temporary directory.")

    # --- ATOMIC DIRECTORY REPLACEMENT ---
    try:
        logging.info(f"Replacing original parquet directory '{wallet_dir}' with temporary directory '{temp_parquet_dir}'...")
        if os.path.exists(wallet_dir):
            shutil.rmtree(wallet_dir) 
            logging.info(f"Existing parquet directory '{wallet_dir}' removed.")
        os.rename(temp_parquet_dir, wallet_dir) 
        logging.info(f"Parquet directory atomically replaced. New directory is: '{wallet_dir}'")
    except OSError as e:
        logging.error(f"Error replacing parquet directory: {e}")
        logging.error(f"Parquet files are still in temporary directory: '{temp_parquet_dir}'")


    duckdb_database_file = os.path.join(parquet_base_dir, "delegator_rewards.duckdb")
    con = duckdb.connect(database=duckdb_database_file)
    logging.info(f"Connected to DuckDB database: {duckdb_database_file}")

    parquet_glob_pattern = os.path.join(parquet_base_dir, "**", "*.parquet")
    try:
        con.execute(f"""
            CREATE OR REPLACE TABLE delegator_claimed_rewards AS
            SELECT * FROM read_parquet('{parquet_glob_pattern}')
        """)
        logging.info(f"DuckDB table 'delegator_claimed_rewards' created/updated from parquet files in: {parquet_base_dir}")
    except Exception as e:
        logging.error(f"Error loading parquet files into DuckDB: {e}")
    finally:
        con.close()
        logging.info("DuckDB connection closed.")


if __name__ == "__main__":
    fetch_reward_claim_history(TARGET_ADDRESS)