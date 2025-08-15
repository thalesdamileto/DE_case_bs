import json
import os

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DoubleType, FloatType,
    BooleanType, TimestampType, DateType, ArrayType, StructType, StructField
)

from brewery_case.domain.models.pipeline_model import PipelineModel


def get_spark_type(type_str):
    type_mapping = {
        "string": StringType(),
        "integer": IntegerType(),
        "long": LongType(),
        "double": DoubleType(),
        "float": FloatType(),
        "boolean": BooleanType(),
        "timestamp": TimestampType(),
        "date": DateType(),
        "array": ArrayType(StringType()),  # Array com tipo interno string (pode ser ajustado)
        "struct": StructType([])  # Placeholder, requer campos específicos no JSON
    }
    return type_mapping.get(type_str.lower(), StringType())


# build sparkSchema from config JSON
def build_schema_from_config(schema_config):
    fields = []
    for field in schema_config["fields"]:
        fields.append(
            StructField(
                name=field["name"],
                dataType=get_spark_type(field["type"]),
                nullable=field.get("nullable", True)
            )
        )
    return StructType(fields)


def get_metadata() -> dict:
    # This json could be retrieved from a metadata MongoDB
    json_file_path = Path(__file__).parent.parent.parent.parent.parent / "pipeline_metadata.json"
    try:
        with open(json_file_path, "r") as file:
            metadata = json.load(file)
    except FileNotFoundError:
        print(f"Erro: Arquivo {json_file_path} não encontrado.")
        raise
    except json.JSONDecodeError:
        print(f"Erro: Falha ao parsear o arquivo JSON {json_file_path}. Verifique a formatação.")
        raise

    return metadata


def config_pipeline_from_metadata() -> PipelineModel:
    metadata = get_metadata()

    pipe_settings = {
        'pipeline_id': metadata['pipeline'].get('pipeline_id', None),
        'pipeline_name': metadata["pipeline"].get('pipeline_name', None),
        'pipeline_parameters': metadata["pipeline"].get('pipeline_parameters', {}),
        'lake_path': metadata["pipeline"].get('lake_path', None),
        'data': {'spark_schema': build_schema_from_config(metadata["pipeline"]["schema"])},
        'dry_run': os.getenv("DRY_RUN", False)
    }

    return PipelineModel(**pipe_settings)
