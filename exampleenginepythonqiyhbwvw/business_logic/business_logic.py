import configparser
from datetime import date
from typing import Tuple

from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as f
from pyspark.sql.types import DateType, StringType, BooleanType, IntegerType, FloatType, LongType, DoubleType, \
    DecimalType


class BusinessLogic:
    """
    Just a wrapper class to ease the user code execution.
    """

    def __init__(self):
        """
        Constructor
        """
        self.__logger = get_user_logger(BusinessLogic.__qualname__)

    # ----------------------------------------------------------------
    # Inician funciones de ejemplos de videos Engine
    def filter_by_age_and_vip(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Apply filter by edad and vip status")
        return df.where((f.col("edad") >= 30) & (f.col("edad") <= 50) & (f.col("vip") == "true"))

    def join_tables(self, clients_df: DataFrame, contracts_df: DataFrame, products_df: DataFrame) -> DataFrame:
        self.__logger.info("Aplicando Join")
        return clients_df.join(contracts_df, f.col("cod_client") == f.col("cod_titular"), "inner") \
            .join(products_df, ["cod_producto"], "inner")

    def filtered_by_number_of_contracts(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filtering by number of contracts")
        return df.select(*df.columns, f.count("cod_client").over(Window.partitionBy("cod_client")).alias("count")) \
            .where(f.col("count") > 3) \
            .drop("count")

    def hash_column(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Generating hash column")
        return df.select(*df.columns, f.sha2(f.concat_ws("||", *df.columns), 256).alias("hash"))

    def select_all_colums(self, df: DataFrame) -> DataFrame:
        return df.select(f.col("cod_producto").cast(StringType()),
                         f.col("cod_iuc").cast(StringType()),
                         f.col("cod_titular").cast(StringType()),
                         f.col("fec_alta").cast(DateType()),
                         f.col("activo").cast(BooleanType()),
                         f.col("cod_client").cast(StringType()),
                         f.col("nombre").cast(StringType()),
                         f.col("edad").cast(IntegerType()),
                         f.col("provincia").cast(StringType()),
                         f.col("cod_postal").cast(IntegerType()),
                         f.col("vip").cast(BooleanType()),
                         f.col("desc_producto").cast(StringType()),
                         f.col("hash").cast(StringType()))

    # ----------------------------------------------------------------
    # Inician funciones de ejercicios propuestos
    def count_cols(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Count columns")
        count_df = df.select([f.count(c).alias(c) for c in df.columns])
        return count_df

    def filtered_by_phone(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filtering by phone")
        df = df.withColumn("cutoff_date", f.col("cutoff_date"))

        start_date = "2020-03-01"
        end_date = "2020-03-04"

        excluded_brands = ["Dell", "Coolpad", "Chea", "BQ", "BLU"]
        excluded_countries = ["CH", "IT", "CZ", "DK"]

        filtered_df = df.filter(
            (f.col("cutoff_date").between(start_date, end_date)) &
            (~f.col("brand").isin(excluded_brands)) &
            (~f.col("country_code").isin(excluded_countries))
        )
        return filtered_df

    def filtered_by_customers(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filtering by customers")
        filtered_df = df.filter(
            (f.col("gl_date").between("2020-03-01", "2020-03-04")) &
            (f.col("credit_card_number").cast("long").isNull() | (f.col("credit_card_number").cast("long") < 1e17))
        )
        return filtered_df

    def join_customer_phone_tables(self, customers_df: DataFrame, phones_df: DataFrame) -> DataFrame:
        self.__logger.info("Join 2 tables")
        joined_df = customers_df.join(phones_df, (["customer_id", "delivery_id"]), "inner")
        return joined_df

    def filtering_vip(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filtering VIP")
        df = df.withColumn("customer_vip",
                           f.when((f.col("prime") == "Yes") & (f.col("price_product") >= 7500.00), "Yes")
                           .otherwise("No"))
        df = df.filter(f.col("customer_vip") == "Yes")
        return df

    def calc_discount(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Calculando descuento")
        df_filtered = df.withColumn("discount_extra", f.when(
            (f.col("prime") == "Yes") &
            (f.col("stock_number") < 35) &
            (~f.col("brand").isin(["XOLO", "Siemens", "Panasonic", "BlackBerry"])),
            f.col("price_product") * 0.1
        ).otherwise(0.00))

        return df_filtered

    def calc_final_price(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Calculo Final Price")
        df = df.withColumn("final_price",
                           f.col("price_product") + f.col("taxes") - f.col("discount_amount") - f.col("discount_extra"))
        average_value = df.select(f.avg("final_price")).first()[0]
        average_df = df.withColumn("average_final_price", f.lit(average_value))
        return average_df

    def count_top_50(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Count top 50:")
        window = Window.partitionBy("brand").orderBy(f.col("final_price").desc())
        df = df.withColumn("rank", f.dense_rank().over(window))
        df = df.withColumn("top_50", f.when(f.col("rank") <= 50, "Entre al top 50").otherwise("No entre al top 50"))
        df = df.filter(f.col("top_50") == "Entre al top 50")
        print(df.count())
        df = df.drop("rank")
        return df

    def replace_nfc(self, df: DataFrame) -> DataFrame:
        df_modified = df.withColumn("nfc", f.when(f.col("nfc").isNull(), "No").otherwise(f.col("nfc")))
        return df_modified

    def count_no_records(self, df: DataFrame) -> int:
        no_count = df.filter(f.col("nfc") == "No").count()
        return no_count

    def add_jwk_date(self, df: DataFrame, field: str) -> DataFrame:
        return df.withColumn("jwk_date", f.lit(field))

    def calc_age(self, df: DataFrame) -> DataFrame:
        current_date = date.today()
        df_age = df.withColumn("age", f.floor(f.datediff(f.lit(current_date), f.col("birth_date")) / 365.25))
        return df_age

    def select_colums(self, df: DataFrame) -> DataFrame:
        return df.select(f.col("city_name").cast(StringType()),
                         f.col("street_name").cast(StringType()),
                         f.col("credit_card_number").cast(StringType()),
                         f.col("last_name").cast(StringType()),
                         f.col("first_name").cast(StringType()),
                         f.col("age").cast(IntegerType()),
                         f.col("brand").cast(StringType()),
                         f.col("model").cast(StringType()),
                         f.col("nfc").cast(StringType()),
                         f.col("country_code").cast(StringType()),
                         f.col("prime").cast(StringType()),
                         f.col("taxes").cast(DecimalType(9, 2)),
                         f.col("discount_extra").cast(DecimalType(9, 2)).alias("extra_discount"),
                         f.col("customer_vip").cast(StringType()),
                         f.col("discount_amount").cast(DecimalType(9, 2)),
                         f.col("price_product").cast(DecimalType(9, 2)),
                         f.col("final_price").cast(DecimalType(9, 2)),
                         f.col("top_50").cast(IntegerType()).alias("brands_top"),
                         f.col("jwk_date").cast(DateType()))
