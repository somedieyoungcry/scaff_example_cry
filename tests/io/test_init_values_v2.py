from unittest import TestCase
from unittest.mock import MagicMock

import pytest
from dataproc_sdk import DatioSchema
from pyspark.sql import DataFrame

from exampleenginepythonqiyhbwvw.common.input import cod_producto, desc_producto
from exampleenginepythonqiyhbwvw.config import get_params_from_runtime
from exampleenginepythonqiyhbwvw.io.init_values import InitValues


class TestApp(TestCase):

    @pytest.fixture(autouse=True)
    def spark_session(self, spark_test):
        self.spark = spark_test

    def test_initialize_inputs(self):
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

        config_loader = self.spark._jvm.com.datio.dataproc.sdk.launcher.process.config.ProcessConfigLoader()
        config = config_loader.fromPath("resources/application.conf")

        runtime_context = MagicMock()
        runtime_context.getConfig.return_value = config
        root_key = "readSchemas"
        parameters = get_params_from_runtime(runtime_context, root_key)

        init_values = InitValues()
        clients_df, contracts_df, products_df, output_path, output_schema = init_values.initialize_inputs(parameters)

        self.assertEqual(type(clients_df), DataFrame)
        self.assertEqual(type(contracts_df), DataFrame)
        self.assertEqual(type(products_df), DataFrame)
        self.assertEqual(type(output_path), str)
        self.assertEqual(type(output_schema), DatioSchema)

        self.assertEqual(products_df.columns, [cod_producto.name, desc_producto.name])

    def get_input_df(self, key_path, key_schema):
        config_loader = self.spark._jvm.com.datio.dataproc.sdk.launcher.process.config.ProcessConfigLoader()
        config = config_loader.fromPath("resources/application.conf")

        runtime_context = MagicMock()
        runtime_context.getConfig.return_value = config
        root_key = "readSchemas"
        parameters = get_params_from_runtime(runtime_context, root_key)

        init_values = InitValues()
        clients_df, contracts_df, products_df, output_path, output_schema = init_values.get_input_df(parameters,
                                                                                                     key_path,
                                                                                                     key_schema)

        self.assertEqual(type(clients_df), DataFrame)
        self.assertEqual(type(contracts_df), DataFrame)
        self.assertEqual(type(products_df), DataFrame)
        self.assertEqual(type(output_path), str)
        self.assertEqual(type(output_schema), DatioSchema)

        self.assertEqual(products_df.columns, [cod_producto.name, desc_producto.name])

    def get_config_by_name(self, key_path, key_schema):
        config_loader = self.spark._jvm.com.datio.dataproc.sdk.launcher.process.config.ProcessConfigLoader()
        config = config_loader.fromPath("resources/application.conf")

        runtime_context = MagicMock()
        runtime_context.getConfig.return_value = config
        root_key = "readSchemas"
        parameters = get_params_from_runtime(runtime_context, root_key)

        init_values = InitValues()
        clients_df, contracts_df, products_df, output_path, output_schema = init_values.get_config_by_name(parameters,
                                                                                                           key_path,
                                                                                                           key_schema)

        self.assertEqual(type(clients_df), DataFrame)
        self.assertEqual(type(contracts_df), DataFrame)
        self.assertEqual(type(products_df), DataFrame)
        self.assertEqual(type(output_path), str)
        self.assertEqual(type(output_schema), DatioSchema)

