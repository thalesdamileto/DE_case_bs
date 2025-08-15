import os

from pyspark.sql import SparkSession

from brewery_case.domain.models.pipeline_model import PipelineModel
from brewery_case.infra.constants.constants import BRONZE_CONTAINER_NAME
from brewery_case.infra.constants.constants import SPARK_READ_BRONZE_PATH

# env vars configs to run spark on local machine
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk-17"
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["PYSPARK_PYTHON"] = "C:/Users/thale/PycharmProjects/DE_case_bs/.venv/Scripts/python.exe"
os.environ["SPARK_HOME"] = "C:/Users/thale/PycharmProjects/DE_case_bs/.venv/Lib/site-packages/pyspark"


def process_data_from_bronze_to_silver(data_pipeline: PipelineModel) -> None:
    spark = SparkSession.builder.getOrCreate()
    read_path = SPARK_READ_BRONZE_PATH.format(bronze_container=BRONZE_CONTAINER_NAME,
                                              folder_path=data_pipeline.lake_path)

    if data_pipeline.dry_run:
        silver_df = spark.createDataFrame(data_pipeline.data.bronze_data, schema=data_pipeline.data.spark_schema)
        data_pipeline.data.silver_data = silver_df
        data_pipeline.data.silver_data.show(truncate=False)

    else:
        silver_df = spark.read.schema(data_pipeline.data.spark_schema).format("json").load(read_path)
        silver_df.write.mode("overwrite").format("delta").saveAsTable('silver.default.brewery_list')
