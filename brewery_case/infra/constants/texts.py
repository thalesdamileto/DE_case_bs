BRONZE_DATA_UPLOADED = "JSON list successfully uploaded to {blob_name} in {container_name}."
BRONZE_PROCESS_START = 'Starting save raw data to bronze.'
PROCESS_FAILED_RETRY = 'Task failed attempt {attempt + 1}/{max_retries}. {error}'
GET_DATA_FROM_API_FAILED = ("Failed to retrieve data from OpenBrewery API. {error}"
                            "Attempt {attempt + 1}/{max_retries}.")
GOLD_START_PROCESS = 'Starting to enrich data from silver to gold.'
FAILED_TO_PARSE_JSON = "Failed to parse JSON: {error}"
PIPELINE_FINISHED = 'Data pipeline successfully finished.'
SILVER_START_PROCESS = 'Starting process data from bronze to silver.'
