from dataclasses import dataclass
from importlib.metadata import version


@dataclass
class GlassFlowConfig:
    """Configuration object for GlassFlowClient

    Attributes:
        server_url: The base URL of the GlassFlow API
        sdk_version: The version of the GlassFlow Python SDK
        user_agent: The user agent to be used in the requests

    """

    server_url: str = "https://api.glassflow.dev/v1"
    sdk_version: str = version("glassflow")
    user_agent: str = f"glassflow-python-sdk/{sdk_version}"
    glassflow_client: str = f"python-sdk/{sdk_version}"
