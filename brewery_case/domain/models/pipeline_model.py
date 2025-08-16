from pydantic import BaseModel
from typing import Any, List, Literal, Optional

from brewery_case.domain.models.logger_model import GeneralLogger


class DataContainer(BaseModel):
    raw_data: Any = None
    bronze_data: Any = None
    silver_data: Any = None
    gold_data:  Any = None
    spark_schema: Any = None
    general_info: Optional[dict] = None


class PipeLineParams(BaseModel):
    priority: Literal["High", "standard", "low"] = "standard"
    custom: Optional[dict] = None


class PipelineModel(BaseModel):
    pipeline_id: str
    pipeline_name: str
    pk_columns: Optional[str] = None
    pipeline_parameters: PipeLineParams
    lake_path: Optional[str] = None
    data: DataContainer = DataContainer()
    logger: GeneralLogger = GeneralLogger()
    dry_run: bool
