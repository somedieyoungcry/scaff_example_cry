from dataproc_sdk import DatioPysparkSession, DatioSchema
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger


class InitValues:
    def __init__(self):
        self.__logger = get_user_logger(InitValues.__qualname__)
        self.__datio_pyspark_session = DatioPysparkSession().get_or_create()

    def initialize_inputs(self, parameters):
        self.__logger.info("Using given configuration")
        clients_df = self.get_input_df(parameters, "clients_path", "clients_schema")
        contracts_df = self.get_input_df(parameters, "contracts_path", "contracts_schema")
        products_df = self.get_input_df(parameters, "products_path", "products_schema")

        customers_df = self.get_parquet_df(parameters, "customers_path", "customers_schema")
        phones_df = self.get_parquet_df(parameters, "phones_path", "phones_schema")

        output_path_2, output_schema_2 = self.get_config_by_name(parameters, "output_path", "output_schema")

        # return clients_df, contracts_df, products_df, output_path, output_schema
        return customers_df, phones_df, output_path_2, output_schema_2

    def initialize_inputs_v2(self, parameters):
        self.__logger.info("Using parquet parameters")

    def get_config_by_name(self, parameters, key_path, key_schema):
        self.__logger.info("Get config for " + key_path)
        io_path = parameters[key_path]
        io_schema = DatioSchema.getBuilder().fromURI(parameters[key_schema]).build()
        return io_path, io_schema

    def get_input_df(self, parameters, key_path, key_schema):
        self.__logger.info("Reading from " + key_path)
        io_path, io_schema = self.get_config_by_name(parameters, key_path, key_schema)
        return self.__datio_pyspark_session.read().datioSchema(io_schema) \
            .option("header", "true") \
            .option("delimiter", ",") \
            .csv(io_path)

    def get_parquet_df(self, parameters, key_path, key_schema):
        self.__logger.info("Leemos el parquet")
        io_path, io_schema = self.get_config_by_name(parameters, key_path, key_schema)
        return self.__datio_pyspark_session.read().datioSchema(io_schema).parquet(io_path)
