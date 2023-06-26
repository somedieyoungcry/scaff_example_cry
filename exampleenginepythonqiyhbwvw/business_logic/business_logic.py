import configparser
from datetime import date
from typing import Tuple

from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as f
from pyspark.sql.types import DateType, StringType, IntegerType, DecimalType
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
    """def filter_by_age_and_vip(self, df: DataFrame) -> DataFrame:
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
        return df.select(*df.columns, f.count(i.cod_client.name).over(Window.partitionBy(i.cod_client.name)).alias(c.COUNT_COLUMN)) \
            .where(f.col(c.COUNT_COLUMN) > 3) \
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
                         o.hash().cast(StringType()))"""

    # ----------------------------------------------------------------
    # Inician funciones de ejercicios propuestos
    def count_cols(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Count columns")
        count_df = df.select([f.count(co).alias(co) for co in df.columns])
        return count_df

    def filtered_by_phone(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filtering by phone")
        df = df.withColumn(i.cutoff_date.name, i.cutoff_date())

        excluded_brands = [c.DELL, c.COOLPAD, c.CHEA, c.BQ, c.BLU]
        excluded_countries = [c.CH, c.IT, c.CZ, c.DK]

        filtered_df = df.filter(
            (i.cutoff_date().between(c.START_DATE, c.END_DATE)) &
            (~i.brand().isin(excluded_brands)) &
            (~i.country_code().isin(excluded_countries))
        )
        return filtered_df

    def filtered_by_customers(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filtering by customers")
        filtered_df = df.filter(
            (f.col("gl_date").between(c.DATE1, c.DATE2)) &
            (f.col("credit_card_number").cast("long").isNull() | (f.col("credit_card_number").cast("long") < 1e17))
        )
        return filtered_df

    def join_customer_phone_tables(self, customers_df: DataFrame, phones_df: DataFrame) -> DataFrame:
        self.__logger.info("Join 2 tables")
        joined_df = customers_df.join(phones_df, ([i.customer_id.name, i.delivery_id.name]), c.INNER_TYPE)
        return joined_df

    def filtering_vip(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Filtering VIP")
        df = df.withColumn(o.customer_vip.name,
                           f.when((o.prime() == c.YES) & (o.price_product() >= 7500.00), c.YES)
                           .otherwise(c.NO))
        df = df.filter(f.col(c.CUSTOMERVIP) == c.YES)
        return df

    def calc_discount(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Calculando descuento")
        df_filtered = df.withColumn(o.extra_discount.name, f.when(
            (i.prime() == c.YES) &
            (i.stock_number() < c.THIRTY5_NUMBER) &
            (~i.brand().isin([c.XOLO, c.SIEMENS, c.PANASONIC, c.BLACKBERRY])),
            i.price_product() * 0.1
        ).otherwise(0.00))

        return df_filtered

    def calc_final_price(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Calculo Final Price")
        df = df.withColumn(o.final_price.name,
                           i.price_product() + i.taxes() - i.discount_amount() - o.extra_discount())
        average_value = df.select(f.avg(c.FINAL_PRICE)).first()[0]
        average_df = df.withColumn(c.AVGFINAL, f.lit(average_value))
        return average_df

    def count_top_50(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Count top 50:")
        window = Window.partitionBy(i.brand.name).orderBy(o.final_price().desc())
        df = df.withColumn(c.RANK, f.dense_rank().over(window))
        df = df.withColumn(c.TOP50, f.when(f.col(c.RANK) <= 50, c.TOP50ENTRY).otherwise(c.TOP50NOENTRY))
        df = df.filter(f.col(c.TOP50) == c.TOP50ENTRY)
        # print(df.count())
        df = df.drop(c.RANK)
        return df

    def replace_nfc(self, df: DataFrame) -> DataFrame:
        df_modified = df.withColumn(o.nfc.name, f.when(o.nfc().isNull(), c.NO).otherwise(o.nfc()))
        return df_modified

    def count_no_records(self, df: DataFrame) -> int:
        no_count = df.filter(f.col(c.NFC) == c.NO).count()
        return no_count

    def add_jwk_date(self, df: DataFrame, field: str) -> DataFrame:
        return df.withColumn(o.jwk_date.name, f.lit(field))

    def calc_age(self, df: DataFrame) -> DataFrame:
        current_date = date.today()
        df_age = df.withColumn(o.age.name, f.floor(f.datediff(f.lit(current_date), * i.birth_date()) / 365.25))
        return df_age

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
                         o.extra_discount().cast(DecimalType(9, 2)).alias(c.EXTRA_DISCOUNT),
                         o.customer_vip().cast(StringType()),
                         o.discount_amount().cast(DecimalType(9, 2)),
                         o.price_product().cast(DecimalType(9, 2)),
                         o.final_price().cast(DecimalType(9, 2)),
                         o.brands_top().cast(IntegerType()).alias(c.BRANDS_TOP),
                         o.jwk_date().cast(DateType()))

