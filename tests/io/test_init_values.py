from unittest.mock import MagicMock

from dataproc_sdk import DatioSchema
from pyspark.sql import DataFrame

from exampleenginepythonqiyhbwvw.common.input import cod_producto, desc_producto
from exampleenginepythonqiyhbwvw.config import get_params_from_runtime
from exampleenginepythonqiyhbwvw.io.init_values import InitValues


def test_initialize_inputs(spark_test):

    """parameters = {
        "clients_path": "resources/data/input/clients.csv",
        "clients_schema": "resources/schemas/clients_schema.json",
        "contracts_path": "resources/data/input/contracts.csv",
        "contracts_schema": "resources/schemas/contracts_schema.json",
        "products_path": "resources/data/input/products.csv",
        "products_schema": "resources/schemas/products_schema.json",
        "output_path": "resources/data/output/final_table",
        "output_schema": "resources/schemas/output_schema.json"
    }"""

    config_loader = spark_test._jvm.com.datio.dataproc.sdk.launcher.process.config.ProcessConfigLoader()
    config = config_loader.fromPath("resources/application.conf")

    runtime_context = MagicMock()
    runtime_context.getConfig.return_value = config
    root_key = "readSchemas"
    parameters = get_params_from_runtime(runtime_context, root_key)

    init_values = InitValues()
    clients_df, contracts_df, products_df, output_path, output_schema = init_values.initialize_inputs(parameters)

    assert type(clients_df) == DataFrame
    assert type(contracts_df) == DataFrame
    assert type(products_df) == DataFrame
    assert type(output_path) == str
    assert type(output_schema) == DatioSchema

    assert products_df.columns == [cod_producto.name, desc_producto.name]
