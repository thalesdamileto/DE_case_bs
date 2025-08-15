import os

BREWERY_API_LIST_URL = os.getenv(
    "BREWERY_API_LIST_URL",
    "https://api.openbrewerydb.org/v1/breweries"
)

DEBUG_ACTIVE = os.getenv("DEBUG_ACTIVE", True)
