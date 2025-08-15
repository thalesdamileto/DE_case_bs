from pydantic import BaseModel
from typing import Any, Literal, Optional

from brewery_case.domain.models.logger_model import GeneralLogger


class PipeLineParams(BaseModel):
    priority: Literal["High", "standard", "low"] = "standard"
    custom: Optional[dict] = None


class PipelineModel(BaseModel):
    pipeline_id: str
    pipeline_name: str
    pipeline_parameters: PipeLineParams
    logger: GeneralLogger = GeneralLogger()
