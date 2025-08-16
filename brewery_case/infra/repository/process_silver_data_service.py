import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, input_file_name
from pyspark.sql.window import Window

from brewery_case.domain.models.pipeline_model import PipelineModel
from brewery_case.infra.constants.texts import SILVER_START_PROCESS, PROCESS_FAILED_RETRY
from brewery_case.infra.constants.constants import BRONZE_CONTAINER_NAME, SILVER_TABLE_NAME, SPARK_READ_BRONZE_PATH, MAX_RETRIES
from brewery_case.infra.repository.helpers.dry_run_helper import dry_run_write_silver_table, dry_run_read_bronze_data, \
    dry_run_move_processed_bronze_data

# env vars configs to run spark on local machine
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk-17"
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["PYSPARK_PYTHON"] = "C:/Users/thale/PycharmProjects/DE_case_bs/.venv/Scripts/python.exe"
os.environ["SPARK_HOME"] = "C:/Users/thale/PycharmProjects/DE_case_bs/.venv/Lib/site-packages/pyspark"


def read_bronze_data_to_transform(data_pipeline: PipelineModel, read_path):
    if data_pipeline.dry_run:
        dry_run_read_bronze_data(data_pipeline)

    else:
        spark = SparkSession.builder.getOrCreate()
        bronze_data = (spark.read
                       .schema(data_pipeline.data.spark_schema)
                       .format("json")
                       .load(read_path)
                       .withColumn("file_path", input_file_name()))
        bronze_read_files = [row.file_path for row in
                             bronze_data.select("file_path").distinct().collect()]
        data_pipeline.data.general_info = {'bronze_read_files': bronze_read_files}
        data_pipeline.data.bronze_data = bronze_data.drop("file_path")


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

    if data_pipeline.logger.debug_active:
        data_pipeline.data.silver_data.show(truncate=False)

    for attempt in range(MAX_RETRIES):
        try:
            if data_pipeline.dry_run:
                dry_run_write_silver_table(data_pipeline=data_pipeline)
                # move files after being processed, so they won't be processed again
                dry_run_move_processed_bronze_data(data_pipeline)

            else:
                data_pipeline.data.silver_data.write.mode("merge").format("delta").saveAsTable(SILVER_TABLE_NAME)

            return

        except Exception as error:
            error_msg = PROCESS_FAILED_RETRY.format(attempt=attempt, max_retries=MAX_RETRIES, error=str(error))
            data_pipeline.logger.warning(error_msg)
            continue

    error_msg = PROCESS_FAILED_RETRY.format(attempt=3, max_retries=MAX_RETRIES, error='max retries')
    data_pipeline.logger.error(error_msg)
    raise Exception(error_msg)


def process_data_from_bronze_to_silver(data_pipeline: PipelineModel) -> None:
    data_pipeline.logger.info(SILVER_START_PROCESS)
    read_path = SPARK_READ_BRONZE_PATH.format(bronze_container=BRONZE_CONTAINER_NAME,
                                              folder_path=data_pipeline.lake_path)

    read_bronze_data_to_transform(data_pipeline, read_path=read_path)

    write_data_to_silver(data_pipeline)
