import os
from brewery_case.domain.models.pipeline_model import PipelineModel
from brewery_case.infra.repository.brewery_api_service import get_brewery_list_from_openbrewerydb
from brewery_case.infra.repository.process_silver_data_service import process_data_from_bronze_to_silver
from brewery_case.infra.repository.save_bronze_data_service import save_data_to_bronze_layer
from brewery_case.infra.repository.services.init_class_from_metadata import config_pipeline_from_metadata
from brewery_case.infra.repository.enrich_gold_data_service import enrich_gold_data_from_silver_to_gold

if __name__ == '__main__':
    try:
        # Initialize Pipeline
        data_pipeline = config_pipeline_from_metadata()

        # GET DATA FROM API
        get_brewery_list_from_openbrewerydb(data_pipeline, full_list=False, list_len=200)

        # SAVE DATA INTO BRONZE
        save_data_to_bronze_layer(data_pipeline)

        # PROCESS DATA FROM BRONZE TO SILVER
        process_data_from_bronze_to_silver(data_pipeline)

        # ENRICH DATA FROM SILVER TO GOLD
        enrich_gold_data_from_silver_to_gold(data_pipeline)

    except Exception as error:
        try:
            data_pipeline.logger.error(message=str(error))

        except:
            print(error)
