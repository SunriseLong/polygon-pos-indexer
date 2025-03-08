import pytest
from indexer.validation import validate_data_against_spec

@pytest.fixture
def valid_event_data():
    return {
        "args": {
            "validatorId": 123,
            "user": "0x1234",
            "rewards": 1000000
        },
        "blockNumber": 12345,
        "transactionHash": b"0x1234567890"
    }

@pytest.fixture
def valid_event_spec():
    return {
        "name": "TestEvent",
        "fields": [
            {"name": "validatorId", "type": int},
            {"name": "user", "type": str},
            {"name": "rewards", "type": int}
        ],
        "root_fields": [
            {"name": "blockNumber", "type": int},
            {"name": "transactionHash", "type": bytes}
        ]
    }

def test_validate_data_against_spec_valid(valid_event_data, valid_event_spec):
    """Test validation with valid data and spec."""
    assert validate_data_against_spec(valid_event_data, valid_event_spec)

def test_validate_data_not_dict():
    """Test validation fails when data is not a dictionary."""
    with pytest.raises(ValueError, match="Data is not a dictionary"):
        validate_data_against_spec(["not a dict"], {})

def test_validate_missing_args():
    """Test validation fails when args field is missing."""
    with pytest.raises(ValueError, match="Data 'args' field is missing or not a dictionary"):
        validate_data_against_spec({"not_args": {}}, {})

def test_validate_args_not_dict():
    """Test validation fails when args is not a dictionary."""
    with pytest.raises(ValueError, match="Data 'args' field is missing or not a dictionary"):
        validate_data_against_spec({"args": "not a dict"}, {})

def test_validate_missing_field(valid_event_data, valid_event_spec):
    """Test validation fails when a required field is missing."""
    del valid_event_data["args"]["validatorId"]
    with pytest.raises(ValueError, match="Missing field in event args: validatorId"):
        validate_data_against_spec(valid_event_data, valid_event_spec)

def test_validate_wrong_field_type(valid_event_data, valid_event_spec):
    """Test validation fails when a field has wrong type."""
    valid_event_data["args"]["validatorId"] = "123"  # string instead of int
    with pytest.raises(TypeError, match="Field 'validatorId' should be of type <class 'int'>, but got <class 'str'>"):
        validate_data_against_spec(valid_event_data, valid_event_spec)

def test_validate_missing_root_field(valid_event_data, valid_event_spec):
    """Test validation fails when a required root field is missing."""
    del valid_event_data["blockNumber"]
    with pytest.raises(ValueError, match="Missing root field in event: blockNumber"):
        validate_data_against_spec(valid_event_data, valid_event_spec)

def test_validate_wrong_root_field_type(valid_event_data, valid_event_spec):
    """Test validation fails when a root field has wrong type."""
    valid_event_data["blockNumber"] = "12345"  # string instead of int
    with pytest.raises(TypeError, match="Root field 'blockNumber' should be of type <class 'int'>, but got <class 'str'>"):
        validate_data_against_spec(valid_event_data, valid_event_spec)

