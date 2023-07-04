from typing import Dict

from dataproc_sdk import DatioPysparkSession
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
import pyspark.sql.functions as f

from exampleenginepythonqiyhbwvw.business_logic.business_logic import BusinessLogic
from exampleenginepythonqiyhbwvw.io.init_values import InitValues
import exampleenginepythonqiyhbwvw.common.constants as c
import exampleenginepythonqiyhbwvw.common.output as o


class DataprocExperiment:
    """
    Just a wrapper class to ease the user code execution.
    """

    def __init__(self):
        """
        Constructor
        """
        self.__logger = get_user_logger(DataprocExperiment.__qualname__)
        self.__logger.info("Ingresamos con Datio SparkSession")
        self.__datio_pyspark_session = DatioPysparkSession().get_or_create()

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

        init_values = InitValues()
        clients_df, contracts_df, products_df, output_path, output_schema = \
            init_values.initialize_inputs(parameters)

        logic = BusinessLogic()
        filtered_clients_df = logic.filter_by_age_and_vip(clients_df)
        joined_df = logic.join_tables(filtered_clients_df, contracts_df, products_df)
        filtered_by_number_of_contracts_df = logic.filtered_by_number_of_contracts(joined_df)
        hashed_df = logic.hash_column(filtered_by_number_of_contracts_df)
        # hashed_df.show()

        final_df = hashed_df \
            .withColumn(o.hash.name, f.when(o.activo() == c.FALSE_VALUE, f.lit(c.ZERO_NUMBER)).otherwise(o.hash())) \
            .where(o.hash() == c.ZERO_NUMBER)

        # final_df.show(20, False)

        self.__datio_pyspark_session.write().mode(c.OVERWRITE) \
            .option(c.MODE, c.DYNAMIC) \
            .partition_by([o.cod_producto.name, o.activo.name]) \
            .datio_schema(output_schema) \
            .parquet(logic.select_all_colums(final_df), output_path)

        """self.__logger.info("Executing Experiment")
        jwk_date = self.get_date("jwk_date", parameters)
        init_values = InitValues()
        customers_df, phones_df, output_path_2, output_schema_2 = \
            init_values.initialize_inputs_v2(parameters)
        # customers_df.show()
        # phones_df.show()
        logic = BusinessLogic()

        # customers_df.printSchema()
        # phones_df.printSchema()

        # customers = logic.count_cols(customers_df)
        # phones = logic.count_cols(phones_df)

        # customers.show()
        # phones.show()

        # print(customers_df.count())
        # print(phones_df.count())

        print("Regla 1")
        phones_filtering = logic.filtered_by_phone(phones_df)
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
        calculate_discount_extra_df = logic.calc_discount(filtering_vip_df)
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
        # nfc_count.show()
        # print(nfc)

        print("Regla 9")
        add_jwk_date_df = logic.add_jwk_date(nfc_count, jwk_date)
        # add_jwk_date_df.show()

        print("Regla 10")
        calc_age_df = logic.calc_age(add_jwk_date_df)
        # calc_age_df.show()
        # print(calc_age_df)
        calc_age_df.printSchema()
        calc_age_df.write \
            .mode("overwrite") \
            .partitionBy("jwk_date") \
            .option("partitionOverwriteMode", "dynamic") \
            .parquet(str(parameters["output_path_2"]))

        self.__datio_pyspark_session.write().mode("overwrite") \
            .option("partitionOverwriteMode", "dynamic") \
            .datio_schema(output_schema_2) \
            .parquet(logic.select_colums(calc_age_df), output_path_2)

    def get_date(self, table_id, parameters):
        return str(parameters[table_id])"""
