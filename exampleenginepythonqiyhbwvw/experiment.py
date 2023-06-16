
from typing import Dict
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from pyspark.sql import SparkSession

class DataprocExperiment:
    """
    Just a wrapper class to ease the user code execution.
    """

    def __init__(self):
        """
        Constructor
        """
        self.__logger = get_user_logger(DataprocExperiment.__qualname__)
        self.__spark = SparkSession.builder.getOrCreate()

    def run(self, **parameters: Dict) -> None:
        """
        Execute the code written by the user.

        Args:
            parameters: The config file parameters
        """
        # -------------------------
        # - Your code starts here -
        # -------------------------
        self.__logger.info("Executing Experiment")
        clients_df = self.read_csv("clients", parameters)
        contracts_df = self.read_csv("contracts", parameters)
        products_df = self.read_csv("products", parameters)
        clients_df.show()
        clients_df.printSchema()

        clients = clients_df.count()
        clients2 = clients_df.collect()
        clients3 = clients_df.head()
        clients4 = clients_df.take(5)
        clients5 = clients_df.first()
        print(clients)
        print(clients2)
        print(clients3)
        print(clients4)
        print(clients5)
        contracts_df.show()
        #products_df.show()

    def read_csv(self, table_id, parameters):
        return self.__spark.read\
            .option("header", "true")\
            .option("delimiter", ",")\
            .csv(str(parameters[table_id]))

