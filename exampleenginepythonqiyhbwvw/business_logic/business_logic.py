import configparser
from datetime import date

from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as f
from pyspark.sql.types import DateType, StringType, IntegerType, DecimalType, BooleanType
import exampleenginepythonqiyhbwvw.common.constants as c
import exampleenginepythonqiyhbwvw.common.input as i
import exampleenginepythonqiyhbwvw.common.output as o


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
        return df.where((i.edad() >= c.THIRTY_NUMBER) &
                        (i.edad() <= c.FIFTY_NUMBER) &
                        (i.vip() == c.TRUE_VALUE))

    def join_tables(self, clients_df: DataFrame, contracts_df: DataFrame, products_df: DataFrame) -> DataFrame:
        self.__logger.info("Aplicando Join")
        return clients_df.join(contracts_df, (f.col("cod_client") == f.col("cod_titular")), c.INNER_TYPE) \
            .join(products_df, [i.cod_producto.name], c.INNER_TYPE)

    def filtered_by_number_of_contracts(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filtering by number of contracts")
        return df.select(*df.columns, f.count(i.cod_client.name)
                         .over(Window.partitionBy(i.cod_client.name)).alias(c.COUNT_COLUMN)) \
            .where(f.col(c.COUNT_COLUMN) > c.THREE_NUMBER) \
            .drop(c.COUNT_COLUMN)

    def hash_column(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Generating hash column")
        return df.select(*df.columns, f.sha2(f.concat_ws(c.CONCAT_SEPARATOR, *df.columns), 256).alias(o.hash.name))

    def select_all_colums(self, df: DataFrame) -> DataFrame:
        return df.select(o.cod_producto().cast(StringType()),
                         o.cod_iuc().cast(StringType()),
                         o.cod_titular().cast(StringType()),
                         o.fec_alta().cast(DateType()),
                         o.activo().cast(BooleanType()),
                         o.cod_client().cast(StringType()),
                         o.nombre().cast(StringType()),
                         o.edad().cast(IntegerType()),
                         o.provincia().cast(StringType()),
                         o.cod_postal().cast(IntegerType()),
                         o.vip().cast(BooleanType()),
                         o.desc_producto().cast(StringType()),
                         o.hash().cast(StringType()))

    # ----------------------------------------------------------------
    # Inician funciones de ejercicios propuestos
    def count_cols(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Count columns")
        count_df = df.select([f.count(co).alias(co) for co in df.columns])
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

    def filter_by_date_phones(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Applying filter by dates and excluding")
        return df.filter((f.col("cutoff_date") >= "2020-03-01") & (f.col("cutoff_date") <= "2020-03-04")
                         & (f.col("brand") != "Dell") & (f.col("brand") != "Coolpad") & (f.col("brand") != "Chea")
                         & (f.col("brand") != "BQ") & (f.col("brand") != "BLU") & (f.col("country_code") != "CH")
                         & (f.col("country_code") != "IT") & (f.col("country_code") != "CZ")
                         & (f.col("country_code") != "DK"))

    def filtered_by_customers(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filtering by customers")
        filtered_df = df.filter(
            (f.col("gl_date").between("2020-03-01", "2020-03-04")) &
            (f.col("credit_card_number").cast("long").isNull() | (f.col("credit_card_number").cast("long") < 1e17))
        )
        return filtered_df

    def join_customer_phone_tables(self, customers_df: DataFrame, phones_df: DataFrame) -> DataFrame:
        self.__logger.info("Join 2 tables")
        joined_df = customers_df.join(phones_df, ([i.customer_id.name, i.delivery_id.name]), c.INNER_TYPE)
        return joined_df

    def filtering_vip(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filtering VIP")
        df = df.filter((f.col("prime") == "Yes") & (f.col("price_product") >= 7500.00))
        df = df.withColumn("customer_vip", f.col("prime"))
        return df

    def calc_discount(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Calculo Discount")
        df_filtered = df.withColumn("discount_extra", f.when(
            (f.col("prime") == "Yes") &
            (f.col("stock_number") < 35) &
            (~f.col("brand").isin(["XOLO", "Siemens", "Panasonic", "BlackBerry"])),
            f.col("price_product") * 0.1
        ).otherwise(0.00)).filter(f.col("discount_extra") > 0)

        return df_filtered

    def calc_final_price(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Calculo Final Price")
        df = df.withColumn("final_price",
                           f.col("price_product") + f.col("taxes") - f.col("discount_amount") - f.col("discount_extra"))

        average = df.select(f.avg("final_price")).first()[0]
        average_df = df.withColumn("average_final_price", f.lit(average))
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
        return df.withColumn(o.jwk_date.name, f.lit(field))

    def calc_age(self, df: DataFrame) -> DataFrame:
        current_date = date.today()
        df = df.withColumn("age", f.floor(f.datediff(f.lit(current_date), f.col("birth_date")) / 365.25))
        return df

    def select_colums(self, df: DataFrame) -> DataFrame:
        return df.select(o.city_name().cast(StringType()),
                         o.street_name().cast(StringType()),
                         o.credit_card_number().cast(StringType()),
                         o.last_name().cast(StringType()),
                         o.first_name().cast(StringType()),
                         o.age().cast(IntegerType()),
                         o.brand().cast(StringType()),
                         o.model().cast(StringType()),
                         o.nfc().cast(StringType()),
                         o.country_code().cast(StringType()),
                         o.prime().cast(StringType()),
                         o.taxes().cast(DecimalType(9, 2)),
                         f.col("discount_extra").cast(DecimalType(9, 2)).alias("extra_discount"),
                         o.customer_vip().cast(StringType()),
                         o.discount_amount().cast(DecimalType(9, 2)),
                         o.price_product().cast(DecimalType(9, 2)),
                         o.final_price().cast(DecimalType(9, 2)),
                         f.col("top_50").cast(IntegerType()).alias("brands_top"),
                         o.jwk_date().cast(DateType()))
