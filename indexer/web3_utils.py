from web3 import Web3
import logging

from environment import PROVIDER_URL

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_web3_connection():
    """Establishes and returns a Web3 connection."""
    w3 = Web3(Web3.HTTPProvider(PROVIDER_URL))
    if not w3.is_connected():
        raise ConnectionError(f"Failed to connect to Ethereum provider: {PROVIDER_URL}")
    logging.info(f"Connected to Ethereum provider")
    return w3

def get_contract_instance(w3, contract_address, contract_abi):
    """Returns a contract instance."""
    contract = w3.eth.contract(address=contract_address, abi=contract_abi)
    logging.info(f"Using contract at: {contract_address}")
    return contract

def fetch_events_in_range(contract_instance, event_name, from_block, to_block, argument_filters=None):
    """Fetches events from a contract within a specified block range."""
    event = getattr(contract_instance.events, event_name)
    event_filter = event.create_filter(
        from_block=from_block,
        to_block=to_block,
        argument_filters=argument_filters
    )
    events = event_filter.get_all_entries()
    return events

