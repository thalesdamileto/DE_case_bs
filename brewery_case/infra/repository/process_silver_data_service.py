from pyspark.sql import SparkSession

from brewery_case.domain.models.pipeline_model import PipelineModel
from brewery_case.infra.constants.constants import BRONZE_CONTAINER_NAME
from brewery_case.infra.constants.constants import SPARK_READ_BRONZE_PATH


def process_data_from_bronze_to_silver(data_pipeline: PipelineModel) -> None:
    spark = SparkSession.builder.getOrCreate()
    read_path = SPARK_READ_BRONZE_PATH.format(bronze_container=BRONZE_CONTAINER_NAME,
                                              folder_path=data_pipeline.lake_path)

    if data_pipeline.dry_run:
        bronze_df = spark.createDataFrame(data_pipeline.data.bronze_data, schema=data_pipeline.data.spark_schema)

    else:
        bronze_df = spark.read.schema(data_pipeline.data.spark_schema).format("json").load(read_path)

    bronze_df.show()
    pass
