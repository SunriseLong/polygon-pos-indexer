import sys
from unittest.mock import MagicMock

mock_env = MagicMock()
mock_env.PROVIDER_URL = "http://localhost:8545" 
sys.modules['environment'] = mock_env 