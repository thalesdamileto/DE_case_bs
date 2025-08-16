BRONZE_BLOB_PATTERN_NAME = "breweries_{unix_timestamp}.json"

BRONZE_CONTAINER_NAME = "bronze"

BRONZE_LOCAL_PATH = 'data/bronze'

BRONZE_LOCAL_PROCESSED_PATH = 'data/bronze/processed'

GOLD_LOCAL_PATH = 'data/gold'

SILVER_LOCAL_PATH = 'data/silver'

SILVER_TABLE_NAME = "silver.default.brewery_list"

SPARK_READ_BRONZE_PATH = "abfss://{bronze_container}@storage_account_name.dfs.core.windows.net/{folder_path}"
