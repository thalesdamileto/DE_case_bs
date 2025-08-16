import json
from datetime import datetime

from azure.storage.blob import BlobServiceClient

from brewery_case.infra.configs.settings import BLOB_STORAGE_ACCOUNT_KEY, BLOB_STORAGE_CONN_STRING
from brewery_case.infra.constants.constants import BRONZE_BLOB_PATTERN_NAME, BRONZE_CONTAINER_NAME, MAX_RETRIES
from brewery_case.infra.constants.texts import BRONZE_DATA_UPLOADED, BRONZE_PROCESS_START, PROCESS_FAILED_RETRY
from brewery_case.infra.repository.helpers.dry_run_helper import dry_run_write_bronze_data
from brewery_case.domain.models.pipeline_model import PipelineModel


def raw_data_to_bronze_data(data_pipeline: PipelineModel) -> None:
    # Convert JSON list to string
    data_pipeline.data.bronze_data = json.dumps(data_pipeline.data.raw_data, indent=2)


def save_data_to_bronze(data_pipeline: PipelineModel, lake_connection_string, container_name, blob_name,
                        full_blob_name) -> None:

    for attempt in range(MAX_RETRIES):
        try:
            if data_pipeline.dry_run:
                dry_run_write_bronze_data(data_pipeline=data_pipeline, blob_name=blob_name)

            else:
                # Initialize BlobServiceClient
                blob_service_client = BlobServiceClient.from_connection_string(lake_connection_string)

                # Get container client
                container_client = blob_service_client.get_container_client(container_name)

                # Create container if it doesn't exist
                container_client.create_container()

                # Upload JSON data to blob
                blob_client = container_client.get_blob_client(full_blob_name)
                blob_client.upload_blob(data_pipeline.data.bronze_data, overwrite=True)
                data_pipeline.logger.info(
                    message=BRONZE_DATA_UPLOADED.format(blob_name=blob_name, container_name=container_name))
            return
        except Exception as error:
            error_msg = PROCESS_FAILED_RETRY.format(attempt=attempt, max_retries=MAX_RETRIES, error=str(error))
            data_pipeline.logger.warning(error_msg)
            continue
    error_msg = PROCESS_FAILED_RETRY.format(attempt=3, max_retries=MAX_RETRIES, error='max retries')
    data_pipeline.logger.error(error_msg)
    raise Exception(error_msg)

def save_data_to_bronze_layer(data_pipeline: PipelineModel) -> None:
    data_pipeline.logger.info(BRONZE_PROCESS_START)
    # Connection details
    lake_connection_string = BLOB_STORAGE_CONN_STRING.format(your_account_key=BLOB_STORAGE_ACCOUNT_KEY)
    container_name = BRONZE_CONTAINER_NAME
    folder_path = data_pipeline.lake_path
    blob_name = BRONZE_BLOB_PATTERN_NAME.format(
        unix_timestamp=str(int(datetime.utcnow().timestamp())))  # adding unix timestamp to file name
    full_blob_name = f"{folder_path}/{blob_name}"

    raw_data_to_bronze_data(data_pipeline)

    save_data_to_bronze(data_pipeline, lake_connection_string, container_name, blob_name, full_blob_name)
