from .pipelines import PipelineClient
from .config import GlassFlowConfig
import requests as requests_http


class GlassFlowClient:

    glassflow_config: GlassFlowConfig

    def __init__(self, organization_id: str = None) -> None:
        rclient = requests_http.Session()
        self.glassflow_config = GlassFlowConfig(rclient)
        self.organization_id = organization_id

    def create_pipeline(self, pipeline: PipelineClient) -> PipelineClient:
        pass

    def pipeline_client(self, space_id: str, pipeline_id: str) -> PipelineClient:
        return PipelineClient(glassflow_client=self, space_id=space_id, pipeline_id=pipeline_id)
