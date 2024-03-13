from dataclasses import dataclass
import requests
from typing import Tuple


@dataclass
class GlassFlowConfig:
    """Configuration object for GlassFlowClient

    Attributes:
        client: requests.Session object to interact with the GlassFlow API
        server_url: The base URL of the GlassFlow API
        sdk_version: The version of the GlassFlow Python SDK
        user_agent: The user agent to be used in the requests

    """
    client: requests.Session
    server_url: str = 'https://api.glassflow.xyz/v1'
    sdk_version: str = '0.0.2'
    user_agent: str = 'glassflow-python-sdk'
