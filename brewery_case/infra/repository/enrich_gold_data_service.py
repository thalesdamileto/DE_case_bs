from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit

from brewery_case.domain.models.pipeline_model import PipelineModel
from brewery_case.infra.constants.constants import SILVER_TABLE_NAME


def read_silver_data_to_transform(data_pipeline: PipelineModel):
    spark = SparkSession.builder.getOrCreate()

    if data_pipeline.dry_run:
        silver_df = data_pipeline.data.silver_data

    else:
        silver_df = spark.read.format("delta").table(SILVER_TABLE_NAME)

    data_pipeline.data.silver_data = silver_df


def create_gold_data_brewery_by_type(data_pipeline: PipelineModel):
    agg_by_type_df = data_pipeline.data.silver_data.groupBy("brewery_type").count().orderBy("count", ascending=False)
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
    agg_by_location_df_B = data_pipeline.data.silver_data.groupBy("state", "city").count().orderBy("count",
                                                                                                   ascending=False)
    agg_by_location_df_A.show(truncate=False)

    enriched_data = {"table": "total_breweries_by_location",
                     "dataset": agg_by_location_df_A}
    data_pipeline.data.gold_data.append(enriched_data)


def write_gold_data(data_pipeline: PipelineModel):
    spark = SparkSession.builder.getOrCreate()
    for dataset in data_pipeline.data.gold_data:
        if data_pipeline.dry_run:
            gold_df_name = dataset.get('table')
            gold_df = dataset.get('dataset')

        else:
            silver_df = spark.read.format("delta").table(SILVER_TABLE_NAME)


def enrich_gold_data_from_silver_to_gold(data_pipeline: PipelineModel) -> None:
    read_silver_data_to_transform(data_pipeline)

    data_pipeline.data.gold_data = []

    create_gold_data_brewery_by_type(data_pipeline)

    create_gold_data_brewery_by_location(data_pipeline)

    write_gold_data(data_pipeline)
