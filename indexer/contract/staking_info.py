import pyarrow as pa
from web3_utils import get_web3_connection
from .base_contract import BaseContract

class StakingInfo(BaseContract):
    """Class encapsulating StakingInfo contract data and schemas."""
    CONTRACT_ADDRESS = "0xa59C847Bd5aC0172Ff4FE912C5d29E5A71A7512B"
    
    # Event definitions
    DELEGATOR_CLAIMED_REWARDS = "DelegatorClaimedRewards"
    
    # ABI for all events from StakingInfo
    _ABI = [
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
            "name": DELEGATOR_CLAIMED_REWARDS,
            "type": "event",
        }
        # Add more events here as needed
    ]

    # Data validation specifications for all events
    _EVENT_SPECS = {
        DELEGATOR_CLAIMED_REWARDS: {
            "name": DELEGATOR_CLAIMED_REWARDS,
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
        # Add more event specs here as needed
    }

    # PyArrow schemas for all events
    _EVENT_SCHEMAS = {
        DELEGATOR_CLAIMED_REWARDS: pa.schema([
            pa.field('block_number', pa.uint64(), nullable=False),
            pa.field('transaction_hash', pa.string(), nullable=False),
            pa.field('validator_id', pa.uint64(), nullable=False),
            pa.field('user_address', pa.string(), nullable=False),
            pa.field('reward_amount_matic', pa.float64(), nullable=False)
        ])
        # Add more event schemas here as needed
    }

    def __init__(self):
        """Initialize StakingInfo with a web3 connection."""
        self.w3 = get_web3_connection()

    @property
    def ABI(self) -> list:
        return self._ABI

    @property
    def event_specs(self) -> dict:
        return self._EVENT_SPECS

    @property
    def event_schemas(self) -> dict:
        return self._EVENT_SCHEMAS

    def process_event_data(self, event_name: str, event_dict: dict) -> dict:
        """Process event data based on the event type."""
        if event_name not in self.supported_events:
            raise ValueError(f"Unsupported event: {event_name}")
            
        if event_name == self.DELEGATOR_CLAIMED_REWARDS:
            return {
                'block_number': event_dict['blockNumber'],
                'transaction_hash': event_dict['transactionHash'].hex(),
                'validator_id': event_dict['args']['validatorId'],
                'user_address': event_dict['args']['user'],
                'reward_amount_matic': float(self.w3.from_wei(event_dict['args']['rewards'], 'ether'))
            }
        
        # Add more event processing here as needed
        raise ValueError(f"Event {event_name} is supported but has no processing implementation")

    @classmethod
    def get_contract_data(cls):
        """Get combined contract information."""
        return {
            "ABI": cls._ABI,
            "EVENT_SPECS": cls._EVENT_SPECS,
            "EVENT_SCHEMAS": cls._EVENT_SCHEMAS,
        }