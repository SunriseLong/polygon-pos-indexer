import pytest
from unittest.mock import Mock, patch
from web3 import Web3

from indexer.web3_utils import get_web3_connection, get_contract_instance, fetch_events_in_range

@pytest.fixture(autouse=True)
def mock_environment():
    """Fixture to ensure environment variables are mocked."""
    with patch('indexer.web3_utils.PROVIDER_URL', 'http://localhost:8545'):
        yield

@pytest.fixture
def mock_web3():
    """Fixture for mocked Web3 instance."""
    with patch('indexer.web3_utils.Web3') as mock_web3_class:
        mock_instance = Mock(spec=Web3)
        
        mock_provider = Mock()
        mock_web3_class.HTTPProvider = Mock(return_value=mock_provider)
        
        mock_web3_class.return_value = mock_instance
        
        mock_instance.eth = Mock()
        mock_instance.eth.contract = Mock()
        
        yield mock_instance, mock_web3_class

@pytest.fixture
def mock_contract():
    """Fixture for mocked contract instance."""
    contract = Mock()
    contract.events = Mock()
    return contract

def test_get_web3_connection_success(mock_web3):
    """Test successful Web3 connection."""
    mock_instance, _ = mock_web3
    mock_instance.is_connected.return_value = True
    
    w3 = get_web3_connection()
    
    assert w3 is not None
    mock_instance.is_connected.assert_called_once()

def test_get_contract_instance(mock_web3):
    """Test contract instance creation."""
    mock_instance, _ = mock_web3
    contract_address = "0x1234567890123456789012345678901234567890"
    contract_abi = [{"type": "function", "name": "test"}]
    
    mock_contract = Mock()
    mock_instance.eth.contract.return_value = mock_contract
    
    contract = get_contract_instance(mock_instance, contract_address, contract_abi)
    
    assert contract is not None
    mock_instance.eth.contract.assert_called_once_with(
        address=contract_address,
        abi=contract_abi
    )

def test_fetch_events_in_range(mock_contract):
    """Test event fetching."""
    event_name = "TestEvent"
    from_block = 1000
    to_block = 2000
    
    mock_event = Mock()
    mock_filter = Mock()
    mock_entries = [{"event": "TestEvent", "args": {}}]
    
    mock_filter.get_all_entries.return_value = mock_entries
    mock_event.create_filter.return_value = mock_filter
    setattr(mock_contract.events, event_name, mock_event)
    
    events = fetch_events_in_range(
        mock_contract,
        event_name,
        from_block,
        to_block
    )
    
    assert events == mock_entries
    mock_event.create_filter.assert_called_once_with(
        from_block=from_block,
        to_block=to_block,
        argument_filters=None
    ) 