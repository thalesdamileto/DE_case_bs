import requests

from brewery_case.domain.models.pipeline_model import PipelineModel
from brewery_case.infra.configs.settings import BREWERY_API_LIST_URL, DEBUG_ACTIVE
from brewery_case.infra.constants.texts import GET_DATA_FROM_API_FAILED

debug = DEBUG_ACTIVE


def get_brewery_list_from_openbrewerydb(data_pipeline: PipelineModel, full_list: bool = False,
                                        list_len: int = 3) -> None:
    data_pipeline.logger.info('Starting get data from OpenBrewery API.')
    api_url = BREWERY_API_LIST_URL

    if not full_list:
        api_url = api_url + f"?per_page={list_len}"

    # Send GET request to the API
    response = requests.get(api_url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        breweries = response.json()
        if debug:
            for brewery in breweries[:2]:
                data_pipeline.logger.debug(message=str(brewery))
        data_pipeline.data.raw_data = breweries

    else:
        data_pipeline.logger.warning(message=GET_DATA_FROM_API_FAILED.format(error_code=response.status_code))
        raise response.status_code
