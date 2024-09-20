from .client import APIClient
from .models.api import GetDetailedSpacePipeline


class Pipeline(APIClient, GetDetailedSpacePipeline):
    def __init__(self, personal_access_token: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.personal_access_token = personal_access_token
