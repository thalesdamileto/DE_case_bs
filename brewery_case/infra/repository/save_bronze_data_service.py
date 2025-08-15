from datetime import datetime
import json

from azure.storage.blob import BlobServiceClient

from brewery_case.infra.configs.settings import BLOB_STORAGE_ACCOUNT_KEY, BLOB_STORAGE_CONN_STRING
from brewery_case.infra.constants.constants import BRONZE_BLOB_PATTERN_NAME, BRONZE_CONTAINER_NAME
from brewery_case.infra.constants.texts import BRONZE_DATA_UPLOADED, BRONZE_DATA_UPLOAD_SKIPPED
from brewery_case.domain.models.pipeline_model import PipelineModel


def save_data_to_bronze_layer(data_pipeline: PipelineModel) -> None:
    # Connection details
    connection_string = BLOB_STORAGE_CONN_STRING.format(your_account_key=BLOB_STORAGE_ACCOUNT_KEY)
    container_name = BRONZE_CONTAINER_NAME
    folder_path = data_pipeline.lake_path
    blob_name = BRONZE_BLOB_PATTERN_NAME.format(
        unix_timestamp=str(int(datetime.utcnow().timestamp())))  # adding unix timestamp to file name
    full_blob_name = f"{folder_path}/{blob_name}"

    # Convert JSON list to string
    json_string = json.dumps(data_pipeline.data.bronze_data, indent=2)

    if not data_pipeline.dry_run:
        # Initialize BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Get container client
        container_client = blob_service_client.get_container_client(container_name)

        # Create container if it doesn't exist
        container_client.create_container()

        # Upload JSON data to blob
        blob_client = container_client.get_blob_client(full_blob_name)
        blob_client.upload_blob(json_string, overwrite=True)
        data_pipeline.logger.info(
            message=BRONZE_DATA_UPLOADED.format(blob_name=blob_name, container_name=container_name))

    else:
        data_pipeline.logger.debug(message=BRONZE_DATA_UPLOAD_SKIPPED)
