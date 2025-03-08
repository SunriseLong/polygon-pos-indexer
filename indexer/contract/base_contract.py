from abc import ABC, abstractmethod
from typing import Dict, Any
import pyarrow as pa

class BaseContract(ABC):
    """Base class for all contract event processors."""

    @property
    @abstractmethod
    def ABI(self) -> list:
        """Contract ABI definition containing all events."""
        pass

    @property
    @abstractmethod
    def event_specs(self) -> Dict[str, dict]:
        """
        Mapping of event names to their specifications.
        Each spec should define the event's validation rules.
        """
        pass

    @property
    @abstractmethod
    def event_schemas(self) -> Dict[str, pa.Schema]:
        """
        Mapping of event names to their output schemas.
        Defines how each event's data will be stored.
        """
        pass

    @property
    def supported_events(self) -> list[str]:
        """List of event names supported by this contract."""
        return list(self.event_specs.keys())

    @abstractmethod
    def process_event_data(self, event_name: str, event_dict: dict) -> dict:
        """
        Process event data into the format specified by its schema.
        
        Args:
            event_name: Name of the event being processed
            event_dict: Raw event data dictionary
            
        Returns:
            Processed event data matching the event's schema
            
        Raises:
            ValueError: If event_name is not supported
        """
        pass

    @classmethod
    def get_contract_data(cls):
        """Get combined contract information."""
        return {
            "ABI": cls.ABI,
            "event_specs": cls.event_specs,
            "event_schemas": cls.event_schemas,
        }