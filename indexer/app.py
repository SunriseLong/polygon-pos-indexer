import logging
from typing import NoReturn
from environment import PROVIDER_URL, TARGET_ADDRESS
from event_processor import EventProcessor, EventProcessorConfig
from contract.staking_info import StakingInfo

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main() -> NoReturn:
    """
    Main function to run the reward claim history fetcher.
    
    Raises:
        ValueError: If any required environment variables are not set.
    """
    if not PROVIDER_URL:
        raise ValueError("PROVIDER_URL is not set in environment")
    if not TARGET_ADDRESS:
        raise ValueError("TARGET_ADDRESS is not set in environment")

    logging.info("Starting Polygon POS Indexer")
    
    config = EventProcessorConfig(
        target_address=TARGET_ADDRESS,
        contract_address=StakingInfo.CONTRACT_ADDRESS,
        # Optionally override defaults:
        start_block=19000000,
        # event_names=["DelegatorClaimedRewards"],
        # block_increment=1000000,
        # output_dir="delegator_claimed_rewards"
    )
    
    processor = EventProcessor(config)
    processor.process_history()
    
    logging.info("Polygon POS Indexer Finished")

if __name__ == "__main__":
    main()
