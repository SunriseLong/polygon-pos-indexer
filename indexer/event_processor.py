import os
import logging
from dataclasses import dataclass
from typing import Type, Optional, List, Dict, Any
from contract.base_contract import BaseContract
from contract.staking_info import StakingInfo
from duckdb_integration import update_duckdb_from_parquet
from web3_utils import get_web3_connection, get_contract_instance, fetch_events_in_range
from parquet_utils import write_events_to_parquet, generate_parquet_filepath, setup_temporary_directory, atomic_directory_replace
from validation import validate_data_against_spec

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@dataclass
class EventProcessorConfig:
    target_address: str
    contract_address: str
    event_names: List[str] = None  # If None, process all supported events
    start_block: int = 0
    block_increment: int = 1000000
    output_dir: str = "contract_events"

class EventProcessor:
    """Handles processing of blockchain events."""
    
    def __init__(self, config: EventProcessorConfig, contract_class: Type[BaseContract] = StakingInfo) -> None:
        self.config = config
        self.w3 = get_web3_connection()
        self.contract_instance = contract_class()
        self.contract = get_contract_instance(
            self.w3, 
            self.config.contract_address, 
            self.contract_instance.ABI
        )
        self.checksum_address = self.w3.to_checksum_address(config.target_address)
        
        self.event_names = (
            self.config.event_names 
            if self.config.event_names is not None 
            else self.contract_instance.supported_events
        )
        
        unsupported = set(self.event_names) - set(self.contract_instance.supported_events)
        if unsupported:
            raise ValueError(f"Unsupported events: {unsupported}")

    def _process_event(self, event_name: str, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single event."""
        try:
            event_dict = dict(event)
            event_dict['args'] = dict(event_dict['args'])
            validate_data_against_spec(event_dict, self.contract_instance.event_specs[event_name])
            return self.contract_instance.process_event_data(event_name, event_dict)
        except (ValueError, TypeError) as e:
            logging.error(f"Validation error processing {event_name} in block {event_dict.get('blockNumber', 'Unknown')}: {e}")
            return None

    def _process_block_range(self, start_block: int, temp_dir: str) -> int:
        """Process events in a block range."""
        end_block = min(start_block + self.config.block_increment, self.w3.eth.block_number + 1)
        if start_block >= end_block:
            return end_block

        logging.info(f"Processing Block Range: {start_block}-{end_block - 1}")
        
        for event_name in self.event_names:
            filepath = generate_parquet_filepath(
                temp_dir,
                start_block, 
                end_block - 1
            )
            
            events = fetch_events_in_range(
                self.contract,
                event_name,
                start_block,
                end_block - 1,
                {'user': self.checksum_address}
            )
            
            processed_events = [
                processed for event in events 
                if (processed := self._process_event(event_name, event)) is not None
            ]
            
            if processed_events:
                write_events_to_parquet(
                    filepath, 
                    processed_events, 
                    self.contract_instance.event_schemas[event_name]
                )
                logging.info(f"Wrote {len(processed_events)} {event_name} events to {filepath}")
        
        return end_block

    def process_history(self) -> None:
        """Process the complete history of events."""
        logging.info(f"Fetching events for {self.config.target_address} from block {self.config.start_block}")
        logging.info(f"Processing events: {', '.join(self.event_names)}")
        
        for event_name in self.event_names:
            temp_dir, final_dir = setup_temporary_directory(
                self.config.output_dir,
                self.config.target_address,
                event_name
            ), os.path.join(self.config.output_dir, event_name.lower(), self.config.target_address)
            
            current_block = self.config.start_block
            while current_block <= self.w3.eth.block_number:
                current_block = self._process_block_range(current_block, temp_dir)
            
            if atomic_directory_replace(temp_dir, final_dir):
                logging.info(f"Successfully updated events for {event_name} in {final_dir}")
        
        update_duckdb_from_parquet(self.config.output_dir)
        logging.info(f"Successfully updated events database in {self.config.output_dir}")

def process_contract_events(
    target_address: str,
    contract_address: str,
    event_names: List[str] = None,
    contract_class: Type[BaseContract] = StakingInfo,
    **kwargs: Any
) -> None:
    """
    Process events from a contract
    
    Args:
        target_address: Address to filter events for
        contract_address: Address of the contract to process events from
        event_names: List of event names to process, or None for all supported events
        contract_class: Contract class to use for processing events
        **kwargs: Additional configuration options for EventProcessorConfig, see EventProcessorConfig for more details
    """
    config = EventProcessorConfig(
        target_address=target_address,
        contract_address=contract_address,
        event_names=event_names,
        **kwargs
    )
    processor = EventProcessor(config, contract_class)
    processor.process_history()
