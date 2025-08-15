
from brewery_case.domain.models.pipeline_model import PipelineModel
from brewery_case.infra.repository.brewery_api_service import get_brewery_list_from_openbrewerydb


if __name__ == '__main__':
    # Initialize Pipeline
    pipe_settings = {
        'pipeline_id': 'CP0001',
        'pipeline_name': 'brewery_pipeline',
        'pipeline_parameters': {
            'priority': 'standard'
        }
    }

    data_pipeline = PipelineModel(**pipe_settings)

    # GET DATA FROM API
    brewery_list = get_brewery_list_from_openbrewerydb(data_pipeline, full_list=False, list_len=10)
    print(brewery_list)

    # SAVE DATA INTO BRONZE

    # PROCESS DATA FROM BRONZE TO SILVER

    # ENRICH DATA FROM SILVER TO GOLD
