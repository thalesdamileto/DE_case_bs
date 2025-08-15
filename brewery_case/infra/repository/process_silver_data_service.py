import os
import json
from pyspark.sql import SparkSession

from brewery_case.domain.models.pipeline_model import PipelineModel
from brewery_case.infra.constants.constants import BRONZE_CONTAINER_NAME, SILVER_TABLE_NAME, SPARK_READ_BRONZE_PATH, \
    BRONZE_LOCAL_PATH
from brewery_case.infra.configs.settings import ROOT_LOCAL_PATH

# env vars configs to run spark on local machine
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk-17"
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["PYSPARK_PYTHON"] = "C:/Users/thale/PycharmProjects/DE_case_bs/.venv/Scripts/python.exe"
os.environ["SPARK_HOME"] = "C:/Users/thale/PycharmProjects/DE_case_bs/.venv/Lib/site-packages/pyspark"


def read_bronze_data_to_transform(data_pipeline: PipelineModel, read_path):
    spark = SparkSession.builder.getOrCreate()

    if data_pipeline.dry_run:
        bronze_data = []
        bronze_read_files = []
        bronze_local_read_path = ROOT_LOCAL_PATH / BRONZE_LOCAL_PATH
        bronze_files_list = list(bronze_local_read_path.glob("*.json"))
        for bronze_file in bronze_files_list:
            with bronze_file.open("r", encoding="utf-8") as file:
                file_data = json.load(file)

            if isinstance(file_data, list):
                bronze_data.extend(file_data)
            else:
                bronze_data.append(file_data)
            bronze_read_files.append(bronze_file)

    else:
        bronze_data = spark.read.schema(data_pipeline.data.spark_schema).format("json").load(read_path)

    data_pipeline.data.general_info = {'bronze_read_files': bronze_read_files}
    data_pipeline.data.bronze_data = bronze_data


def move_processed_data(data_pipeline: PipelineModel):
    pass


def write_data_to_silver(data_pipeline: PipelineModel):
    spark = SparkSession.builder.getOrCreate()

    if isinstance(data_pipeline.data.bronze_data, list):
        silver_df = spark.createDataFrame(data_pipeline.data.bronze_data, schema=data_pipeline.data.spark_schema)

    if data_pipeline.dry_run:
        data_pipeline.data.silver_data = silver_df
        data_pipeline.data.silver_data.show(truncate=False)

    else:
        silver_df.write.mode("overwrite").format("delta").saveAsTable(SILVER_TABLE_NAME)

    move_processed_data(data_pipeline)


def process_data_from_bronze_to_silver(data_pipeline: PipelineModel) -> None:
    spark = SparkSession.builder.getOrCreate()
    read_path = SPARK_READ_BRONZE_PATH.format(bronze_container=BRONZE_CONTAINER_NAME,
                                              folder_path=data_pipeline.lake_path)

    read_bronze_data_to_transform(data_pipeline, read_path=read_path)

    write_data_to_silver(data_pipeline)
