BRONZE_BLOB_PATTERN_NAME = "breweries_{unix_timestamp}.json"

BRONZE_CONTAINER_NAME = "bronze"

BRONZE_LOCAL_PATH = 'data/bronze'

BRONZE_LOCAL_PROCESSED_PATH = 'data/bronze/processed'

DEFAULT_LIST_LEN = 200

GOLD_LOCAL_PATH = 'data/gold'

MAX_RETRIES = 3

SILVER_LOCAL_PATH = 'data/silver'

SILVER_TABLE_NAME = "silver.default.brewery_list"

SPARK_READ_BRONZE_PATH = "abfss://{bronze_container}@storage_account_name.dfs.core.windows.net/{folder_path}"
