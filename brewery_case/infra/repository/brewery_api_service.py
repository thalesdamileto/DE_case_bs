import requests

from brewery_case.domain.models.pipeline_model import PipelineModel
from brewery_case.infra.configs.settings import BREWERY_API_LIST_URL, DEBUG_ACTIVE

debug = DEBUG_ACTIVE


def get_brewery_list_from_openbrewerydb(data_pipeline: PipelineModel, full_list: bool = False, list_len: int = 3) -> list:
    api_url = BREWERY_API_LIST_URL

    if not full_list:
        api_url = api_url+f"?per_page={list_len}"

    # Send GET request to the API
    response = requests.get(api_url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON response
        breweries = response.json()
        if debug:
            for brewery in breweries:
                data_pipeline.logger.debug(message=str(brewery))
        return breweries

    else:
        data_pipeline.logger.warning(message=f"Failed to retrieve data. Status code: {response.status_code}")
        raise response.status_code
