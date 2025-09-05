import configparser
import os
import uuid
from pathlib import Path

from pydantic import BaseModel


class GlassFlowSettings(BaseModel):
    host: str = "http://localhost:8080"


class AnalyticsSettings(BaseModel):
    enabled: bool = True
    distinct_id: str = str(uuid.uuid4())


class GlassFlowConfig(BaseModel):
    config_file: Path = Path.home() / ".glassflow" / "clickhouse.conf"
    glassflow: GlassFlowSettings = GlassFlowSettings()
    analytics: AnalyticsSettings = AnalyticsSettings()

    def model_post_init(self, ctx):
        self.load()

    def load(self):
        config = configparser.ConfigParser()

        if os.path.exists(self.config_file):
            with open(self.config_file, "r") as f:
                config.read_file(f)
            if "glassflow" in config:
                self.glassflow = GlassFlowSettings(**config["glassflow"])
            if "analytics" in config:
                self.analytics = AnalyticsSettings(**config["analytics"])
        else:
            self.write()

    def write(self):
        if not os.path.exists(self.config_file.parent):
            os.makedirs(self.config_file.parent)

        config = configparser.ConfigParser()
        config["glassflow"] = self.glassflow.model_dump()
        config["analytics"] = self.analytics.model_dump()

        with open(self.config_file, "w") as f:
            config.write(f)
