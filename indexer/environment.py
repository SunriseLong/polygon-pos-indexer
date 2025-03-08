import os
from dotenv import dotenv_values

env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")

config = {
    **dotenv_values(env_path), # load variables from .env file
    **os.environ,  # override loaded values with environment variables
}

PROVIDER_URL = config.get("PROVIDER_URL")
TARGET_ADDRESS = config.get("TARGET_ADDRESS")
STAKING_INFO_CONTRACT_ADDRESS = config.get("STAKING_INFO_CONTRACT_ADDRESS")
