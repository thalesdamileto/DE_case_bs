from pyspark.sql import SparkSession

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
    data_pipeline.data.gold_data.append(agg_by_type_df)


def create_gold_data_brewery_by_location(data_pipeline: PipelineModel):
    # agg_by_location_df = silver_df.withColumn("location", concat(col("city"), lit(" - "), col("state"))).groupBy(
    #     "location").count().orderBy("count", ascending=False)
    agg_by_location_df = data_pipeline.data.silver_data.groupBy("state", "city").count().orderBy("count",
                                                                                                 ascending=False)
    agg_by_location_df.show(truncate=False)
    data_pipeline.data.gold_data.append(agg_by_location_df)


def write_gold_data(data_pipeline: PipelineModel):
    spark = SparkSession.builder.getOrCreate()
    for dataset in data_pipeline.data.gold_data:
        if data_pipeline.dry_run:
            silver_df = data_pipeline.data.silver_data

        else:
            silver_df = spark.read.format("delta").table(SILVER_TABLE_NAME)

        data_pipeline.data.silver_data = silver_df


def enrich_gold_data_from_silver_to_gold(data_pipeline: PipelineModel) -> None:
    read_silver_data_to_transform(data_pipeline)

    data_pipeline.data.gold_data = []

    create_gold_data_brewery_by_type(data_pipeline)

    create_gold_data_brewery_by_location(data_pipeline)

    write_gold_data(data_pipeline)
