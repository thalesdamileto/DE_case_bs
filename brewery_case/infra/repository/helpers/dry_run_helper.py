import json
import os
import pandas as pd
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, row_number, lit
from pyspark.sql.window import Window
from brewery_case.domain.models.pipeline_model import PipelineModel
from brewery_case.infra.configs.settings import ROOT_LOCAL_PATH
from brewery_case.infra.constants.constants import BRONZE_CONTAINER_NAME, SILVER_TABLE_NAME, SPARK_READ_BRONZE_PATH, \
    BRONZE_LOCAL_PATH, BRONZE_LOCAL_PROCESSED_PATH, SILVER_LOCAL_PATH, GOLD_LOCAL_PATH

# env vars configs to run spark on local machine
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk-17"
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["PYSPARK_PYTHON"] = "C:/Users/thale/PycharmProjects/DE_case_bs/.venv/Scripts/python.exe"
os.environ["SPARK_HOME"] = "C:/Users/thale/PycharmProjects/DE_case_bs/.venv/Lib/site-packages/pyspark"

"""  I'm unable to save to a local Delta table because of several errors with Hadoop and JAR packages,
     so I'll be using Parquet with Pandas instead."""


def delete_files_in_directory(directory_path: Path):
    delete_path = Path(directory_path).resolve()
    for file in delete_path.iterdir():
        if file.is_file():
            file.unlink()


def dry_run_write_bronze_data(data_pipeline: PipelineModel, blob_name: str):
    bronze_local_path = ROOT_LOCAL_PATH / BRONZE_LOCAL_PATH
    bronze_local_path.mkdir(parents=True, exist_ok=True)
    bronze_file_path = bronze_local_path / f"{blob_name}"
    bronze_file_path.write_text(data_pipeline.data.bronze_data)


def dry_run_read_bronze_data(data_pipeline: PipelineModel):
    bronze_read_files = []
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

    data_pipeline.data.general_info = {'bronze_read_files': bronze_read_files}
    data_pipeline.data.bronze_data = bronze_data


def dry_run_move_processed_bronze_data(data_pipeline: PipelineModel):
    bronze_local_read_path = ROOT_LOCAL_PATH / BRONZE_LOCAL_PROCESSED_PATH
    read_file_list = data_pipeline.data.general_info.get('bronze_read_files', [])

    for file in read_file_list:
        new_file_path = bronze_local_read_path / file.name
        file.rename(new_file_path)


def dry_run_write_silver_table(data_pipeline: PipelineModel):
    # data_pipeline.data.silver_data.write.format("parquet").mode("overwrite").save(str(silver_local_path))
    silver_file_name = str(str(int(datetime.utcnow().timestamp())))
    silver_local_path = ROOT_LOCAL_PATH / SILVER_LOCAL_PATH

    merge_delta_table(target_path=silver_local_path, pk_column=data_pipeline.pk_columns,
                      new_dataframe=data_pipeline.data.silver_data, table_name=silver_file_name)

    # pandas_silver_df = data_pipeline.data.silver_data.toPandas()
    # pandas_silver_df.to_parquet(str(silver_local_path / f"{silver_file_name}.parquet"), engine="pyarrow",
    #                            index=False)


def dry_run_read_silver_table(data_pipeline: PipelineModel):
    spark = SparkSession.builder.getOrCreate()
    silver_local_read_path = ROOT_LOCAL_PATH / SILVER_LOCAL_PATH
    silver_pandas_df = pd.read_parquet(silver_local_read_path)
    silver_spark_df = spark.createDataFrame(silver_pandas_df, schema=data_pipeline.data.spark_schema)
    data_pipeline.data.silver_data = silver_spark_df


def dry_run_write_gold_table(table_name: str, dataframe: DataFrame):
    gold_local_path = ROOT_LOCAL_PATH / GOLD_LOCAL_PATH / table_name
    pandas_gold_df = dataframe.toPandas()
    delete_files_in_directory(gold_local_path)
    pandas_gold_df.to_parquet(str(gold_local_path / f"{table_name}.parquet"), engine="pyarrow", index=False)


def merge_delta_table(target_path: Path, pk_column: str, new_dataframe: DataFrame, table_name: str):
    spark = SparkSession.builder.getOrCreate()
    schema = new_dataframe.schema
    pd_target_df = pd.read_parquet(target_path)
    target_df = spark.createDataFrame(pd_target_df, schema=schema).withColumn('ordering', lit(2))
    new_dataframe = new_dataframe.withColumn('ordering', lit(1))
    merged_df = target_df.union(new_dataframe)
    window = Window.partitionBy(pk_column).orderBy(col('ordering').asc())
    merged_df = merged_df.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")

    # deleta arquivos no diretorio
    delete_files_in_directory(target_path)

    # escreve novo dataframe após merge
    pandas_merged_df = merged_df.toPandas()
    pandas_merged_df.to_parquet(str(target_path / f"{table_name}.parquet"), engine="pyarrow",
                                index=False)
