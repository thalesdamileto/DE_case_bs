import os

BLOB_STORAGE_ACCOUNT_KEY = os.getenv("BLOB_STORAGE_ACCOUNT_KEY",
                                     "dev-key"
                                     )

BLOB_STORAGE_CONN_STRING = ("DefaultEndpointsProtocol=https;AccountName=storage_account_name;AccountKey={"
                            "your_account_key};EndpointSuffix=core.windows.net")

BREWERY_API_LIST_URL = os.getenv(
    "BREWERY_API_LIST_URL",
    "https://api.openbrewerydb.org/v1/breweries"
)

DEBUG_ACTIVE = os.getenv("DEBUG_ACTIVE", True)
