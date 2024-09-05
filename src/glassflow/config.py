from dataclasses import dataclass
from importlib.metadata import version

import requests


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
    server_url: str = "https://api.glassflow.dev/v1"
    sdk_version: str = version("glassflow")
    user_agent: str = "glassflow-python-sdk/{}".format(sdk_version)
    glassflow_client: str = "python-sdk/{}".format(sdk_version)
