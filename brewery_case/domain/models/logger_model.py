from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel

from brewery_case.infra.configs.settings import DEBUG_ACTIVE


class LogDimensions(BaseModel):
    start_time_utc: str = str(datetime.utcnow())
    service: Optional[str] = None


class GeneralLogger(LogDimensions):
    """Class used to abstract how to log"""

    app_name: str = "app"
    log_dimensions: LogDimensions = LogDimensions()
    log_service_conn_string: Optional[str] = None
    logger: Any = None
    debug_active: bool = DEBUG_ACTIVE

    def __send_log(self, severity_level: str, msg: str,) -> None:
        print(f"{severity_level}: {datetime.utcnow()} - {msg}")

    def debug(self, message: str) -> None:
        if self.debug_active:
            self.__send_log(severity_level="DEBUG", msg=message)

    def info(self, message: str) -> None:
        self.__send_log(severity_level="INFO", msg=message)

    def warning(self, message: str) -> None:
        self.__send_log(severity_level="WARNING", msg=message)

    def error(self, message: str) -> None:
        self.__send_log(severity_level="ERROR", msg=message)
