from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from pyspark.sql import DataFrame
import pyspark.sql.functions as f



class BusinessLogic:
    """
    Just a wrapper class to ease the user code execution.
    """

    def __init__(self):
        """
        Constructor
        """
        self.__logger = get_user_logger(BusinessLogic.__qualname__)

    def filter_by_age_and_vip(self, df: DataFrame) -> DataFrame:
        df.where(f.col())

