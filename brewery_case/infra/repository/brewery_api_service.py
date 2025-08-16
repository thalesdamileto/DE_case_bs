import requests

from requests.exceptions import RequestException

from brewery_case.domain.models.pipeline_model import PipelineModel
from brewery_case.infra.configs.settings import BREWERY_API_LIST_URL
from brewery_case.infra.constants.constants import MAX_RETRIES
from brewery_case.infra.constants.texts import GET_DATA_FROM_API_FAILED, FAILED_TO_PARSE_JSON, PROCESS_FAILED_RETRY


def get_brewery_list_from_openbrewerydb(data_pipeline: PipelineModel, full_list: bool = False,
                                        list_len: int = 3) -> None:
    data_pipeline.logger.info('Starting get data from OpenBrewery API.')
    api_url = BREWERY_API_LIST_URL

    if not full_list:
        api_url = api_url + f"?per_page={list_len}"

    for attempt in range(MAX_RETRIES):
        try:
            # Send GET request to the API
            response = requests.get(api_url, timeout=10)
            response.raise_for_status()

            # Parse the JSON response
            breweries = response.json()
            # Log 2 items in case of debug active
            if data_pipeline.logger.debug_active:
                for brewery in breweries[:2]:
                    data_pipeline.logger.debug(message=str(brewery))

            data_pipeline.data.raw_data = breweries
            return

        except RequestException as error:
            error_msg = GET_DATA_FROM_API_FAILED.format(error=str(error), attempt=attempt, max_retries=MAX_RETRIES)
            data_pipeline.logger.warning(error_msg)
            continue

        except ValueError as error:
            # Erro ao parsear JSON
            error_msg = FAILED_TO_PARSE_JSON.format(error=str(error))
            data_pipeline.logger.error(error_msg)
            raise

    # Após todas as tentativas falharem
    error_msg = PROCESS_FAILED_RETRY.format(attempt=3, max_retries=MAX_RETRIES, error='max retries')
    data_pipeline.logger.error(error_msg)
    raise Exception(error_msg)
