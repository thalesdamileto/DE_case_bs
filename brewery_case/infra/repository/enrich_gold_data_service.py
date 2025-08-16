from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit

from brewery_case.domain.models.pipeline_model import PipelineModel
from brewery_case.infra.constants.constants import SILVER_TABLE_NAME, MAX_RETRIES
from brewery_case.infra.constants.texts import GOLD_START_PROCESS, PROCESS_FAILED_RETRY
from brewery_case.infra.repository.helpers.dry_run_helper import dry_run_write_gold_table, dry_run_read_silver_table


def read_silver_data_to_transform(data_pipeline: PipelineModel):
    spark = SparkSession.builder.getOrCreate()

    if data_pipeline.dry_run:
        dry_run_read_silver_table(data_pipeline)

    else:
        data_pipeline.data.silver_data = spark.read.format("delta").table(SILVER_TABLE_NAME)


def create_gold_data_brewery_by_type(data_pipeline: PipelineModel):
    agg_by_type_df = data_pipeline.data.silver_data.groupBy("brewery_type").count().orderBy("count", ascending=False)
    if data_pipeline.logger.debug_active:
        agg_by_type_df.show(truncate=False)

    enriched_data = {"table": "total_breweries_by_type",
                     "dataset": agg_by_type_df}
    data_pipeline.data.gold_data.append(enriched_data)


def create_gold_data_brewery_by_location(data_pipeline: PipelineModel):
    # This agg method will be better from an architecture pov,
    # because it's easier to partition data by 1 column, and will require less space to be saved.
    agg_by_location_df_A = data_pipeline.data.silver_data.withColumn("location", concat(col("city"), lit(" - "),
                                                                                        col("state"))).groupBy(
        "location").count().orderBy("count", ascending=False)

    # This agg method will be better from an analytical pov, because has a greater granularity level
    # agg_by_location_df_B = data_pipeline.data.silver_data.groupBy("state", "city").count().orderBy("count",
    #                                                                                              ascending=False)

    if data_pipeline.logger.debug_active:
        agg_by_location_df_A.show(truncate=False)

    enriched_data = {"table": "total_breweries_by_location",
                     "dataset": agg_by_location_df_A}
    data_pipeline.data.gold_data.append(enriched_data)


def write_gold_data(data_pipeline: PipelineModel):
    spark = SparkSession.builder.getOrCreate()
    for attempt in range(MAX_RETRIES):
        try:
            for dataset in data_pipeline.data.gold_data:
                if data_pipeline.dry_run:
                    table_name = dataset.get('table', None)
                    dataframe = dataset.get('dataset', None)
                    dry_run_write_gold_table(table_name=table_name, dataframe=dataframe)

                else:
                    silver_df = spark.read.format("delta").table(SILVER_TABLE_NAME)
            return

        except Exception as error:
            error_msg = PROCESS_FAILED_RETRY.format(attempt=attempt, max_retries=MAX_RETRIES, error=str(error))
            data_pipeline.logger.warning(error_msg)
            continue

    error_msg = PROCESS_FAILED_RETRY.format(attempt=3, max_retries=MAX_RETRIES, error='max retries')
    data_pipeline.logger.error(error_msg)
    raise Exception(error_msg)


def enrich_gold_data_from_silver_to_gold(data_pipeline: PipelineModel) -> None:
    data_pipeline.logger.info(GOLD_START_PROCESS)
    read_silver_data_to_transform(data_pipeline)

    data_pipeline.data.gold_data = []

    create_gold_data_brewery_by_type(data_pipeline)

    create_gold_data_brewery_by_location(data_pipeline)

    write_gold_data(data_pipeline)
