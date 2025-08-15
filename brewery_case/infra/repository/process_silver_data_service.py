import os
import json

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

from brewery_case.domain.models.pipeline_model import PipelineModel
from brewery_case.infra.constants.constants import BRONZE_CONTAINER_NAME, SILVER_TABLE_NAME, SPARK_READ_BRONZE_PATH, \
    BRONZE_LOCAL_PATH, BRONZE_LOCAL_PROCESSED_PATH, SILVER_LOCAL_PATH
from brewery_case.infra.configs.settings import ROOT_LOCAL_PATH

# env vars configs to run spark on local machine
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk-17"
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["PYSPARK_PYTHON"] = "C:/Users/thale/PycharmProjects/DE_case_bs/.venv/Scripts/python.exe"
os.environ["SPARK_HOME"] = "C:/Users/thale/PycharmProjects/DE_case_bs/.venv/Lib/site-packages/pyspark"


def read_bronze_data_to_transform(data_pipeline: PipelineModel, read_path):
    spark = SparkSession.builder.getOrCreate()
    bronze_read_files = []

    if data_pipeline.dry_run:
        bronze_data = []
        bronze_local_read_path = ROOT_LOCAL_PATH / BRONZE_LOCAL_PATH

        # Reading all files assure that in fails we will not lose any data
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
    bronze_local_read_path = ROOT_LOCAL_PATH / BRONZE_LOCAL_PROCESSED_PATH
    read_file_list = data_pipeline.data.general_info.get('bronze_read_files', [])

    for file in read_file_list:
        new_file_path = bronze_local_read_path / file.name
        file.rename(new_file_path)


def silver_quality_checks(data_pipeline: PipelineModel):
    pk_columns = data_pipeline.pk_columns.split(',')
    # drop pk_nulls from dataset
    cleansed_df = data_pipeline.data.silver_data.na.drop(subset=pk_columns, how="all")

    # drop duplicates keep one
    window = Window.partitionBy(pk_columns).orderBy(col(pk_columns[0]).desc())
    cleansed_df = cleansed_df.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")

    data_pipeline.data.silver_data = cleansed_df


def write_data_to_silver(data_pipeline: PipelineModel):
    spark = SparkSession.builder.getOrCreate()

    if isinstance(data_pipeline.data.bronze_data, list):
        data_pipeline.data.silver_data = spark.createDataFrame(data_pipeline.data.bronze_data,
                                                               schema=data_pipeline.data.spark_schema)

    silver_quality_checks(data_pipeline)

    if data_pipeline.dry_run:
        # data_pipeline.data.silver_data.write.format("parquet").mode("overwrite").save(str(silver_local_path))
        # I'm unable to save to a local Delta table because of several errors with Hadoop and JAR packages,
        # so I'll be using Parquet with Pandas instead.
        silver_file_name = str(str(int(datetime.utcnow().timestamp())))
        silver_local_path = ROOT_LOCAL_PATH / SILVER_LOCAL_PATH

        pandas_silver_df = data_pipeline.data.silver_data.toPandas()
        pandas_silver_df.to_parquet(str(silver_local_path / f"{silver_file_name}.parquet"), engine="pyarrow",
                                    index=False)

    else:
        data_pipeline.data.silver_data.write.mode("merge").format("delta").saveAsTable(SILVER_TABLE_NAME)

    # move files after being processed, so they won't be processed again
    move_processed_data(data_pipeline)


def process_data_from_bronze_to_silver(data_pipeline: PipelineModel) -> None:
    spark = SparkSession.builder.getOrCreate()
    read_path = SPARK_READ_BRONZE_PATH.format(bronze_container=BRONZE_CONTAINER_NAME,
                                              folder_path=data_pipeline.lake_path)

    read_bronze_data_to_transform(data_pipeline, read_path=read_path)

    write_data_to_silver(data_pipeline)
