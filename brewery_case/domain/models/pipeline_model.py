from pydantic import BaseModel
from typing import Any, Literal, Optional

from brewery_case.domain.models.logger_model import GeneralLogger


class DataContainer(BaseModel):
    bronze_data: Any = None
    silver_data: Any = None
    gold_data: Any = None
    spark_schema: Any = None


class PipeLineParams(BaseModel):
    priority: Literal["High", "standard", "low"] = "standard"
    custom: Optional[dict] = None


class PipelineModel(BaseModel):
    pipeline_id: str
    pipeline_name: str
    pipeline_parameters: PipeLineParams
    lake_path: Optional[str] = None
    data: DataContainer = DataContainer()
    logger: GeneralLogger = GeneralLogger()
    dry_run: bool
