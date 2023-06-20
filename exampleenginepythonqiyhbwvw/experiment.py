from typing import Dict
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f

from exampleenginepythonqiyhbwvw.business_logic.business_logic import BusinessLogic


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
        """self.__logger.info("Executing Experiment")
        clients_df = self.read_csv("clients", parameters)
        contracts_df = self.read_csv("contracts", parameters)
        products_df = self.read_csv("products", parameters)
        # clients_df.show()
        # clients_df.printSchema()

        clients = clients_df.count()
        clients2 = clients_df.collect()
        clients3 = clients_df.head()
        clients4 = clients_df.take(5)
        clients5 = clients_df.first()
        # print(clients)
        # print(clients2)
        # print(clients3)
        # print(clients4)
        # print(clients5)
        # contracts_df.show()

        logic = BusinessLogic()
        filtered_clients_df: DataFrame = logic.filter_by_age_and_vip(clients_df)
        joined_df: DataFrame = logic.join_tables(filtered_clients_df, contracts_df, products_df)
        filtered_by_number_of_contracts_df = logic.filtered_by_number_of_contracts(joined_df)
        hashed_df: DataFrame = logic.hash_column(filtered_by_number_of_contracts_df)
        hashed_df.show(20, False)"""

        self.__logger.info("Executing Experiment")
        customers_df = self.read_parquet("customers", parameters)
        phones_df = self.read_parquet("phones", parameters)
        # customers_df.show()
        # phones_df.show()
        logic = BusinessLogic()

        customers = logic.count_cols(customers_df)
        phones = logic.count_cols(phones_df)

        # customers.show()
        # phones.show()

        # print(customers_df.count())
        # print(phones_df.count())

        phones_filtering = logic.filtered_by_phone(phones_df)
        print("Regla 1")
        # print(phones_filtering.count())
        # phones_filtering.show()

        print("Regla 2")
        filtered_by_customers_df = logic.filtered_by_customers(customers_df)
        # filtered_by_customers_df.show()
        # print(filtered_by_customers_df.count())

        print("Regla 3")
        join_2_tables_df = logic.join_customer_phone_tables(filtered_by_customers_df, phones_filtering)
        # join_2_tables_df.show()
        # print(join_2_tables_df.count())

        print("Regla 4")
        filtering_vip_df = logic.filtering_vip(join_2_tables_df)
        # filtering_vip_df.show()
        # print(filtering_vip_df.count())

        print("Regla 5")
        df_filtered = f.col("discount_extra") > 0
        calculate_discount_extra_df = logic.calc_discount(join_2_tables_df)
        # calculate_discount_extra_df.show()
        # print(calculate_discount_extra_df.filter(df_filtered).count())

        print("Regla 6")
        calculate_final_price_df = logic.calc_final_price(calculate_discount_extra_df)
        # calculate_final_price_df.show()
        # print(calculate_final_price_df.count())

        print("Regla 7")
        count_top_50_records_df = logic.count_top_50(calculate_final_price_df)
        # count_top_50_records_df.show()
        # print(count_top_50_records_df.count())

        print("Regla 8")
        nfc_count = logic.replace_nfc(count_top_50_records_df)
        nfc = logic.count_no_records(nfc_count)
        # print(nfc)

        print("Regla 9")
        add_jwk_date_df = logic.add_jwk_date(join_2_tables_df)
        add_jwk_date_df.select("jwk_date").show()

    def read_csv(self, table_id, parameters):
        return self.__spark.read \
            .option("header", "true") \
            .option("delimiter", ",") \
            .csv(str(parameters[table_id]))

    def read_parquet(self, table_id, parameters):
        return self.__spark.read \
            .parquet(str(parameters[table_id]))
