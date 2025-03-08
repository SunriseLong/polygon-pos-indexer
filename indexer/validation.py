def validate_data_against_spec(data, spec) -> bool:
    """
    Validates data against a given specification.

    Args:
        data (dict): Data to validate.
        spec (dict): Specification defining expected fields and types.
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