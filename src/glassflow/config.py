
from dataclasses import dataclass
import requests
from typing import Dict, Tuple
from .utils import utils


@dataclass
class GlassFlowConfig:
    client: requests.Session
    server_url: str = 'http://api.glassflow.xyz/v1'
    sdk_version: str = '0.0.1'
    user_agent: str = 'glassflow-python-sdk'
