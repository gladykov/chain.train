import pytest
from helpers.setup import setup as my_setup
import importlib


class TestClass:

    @classmethod
    def setup_class(cls):
        print("first home run")
        setup = my_setup()
        cls.env = setup.parser.env
        cls.schema_name = setup.parser.schema_name
        cls.schema = importlib.import_module(f"schemas.{cls.schema_name}").schema_definition.schema_for_environment(cls.env)

    def test_1(self):
        print(self.schema.table_names())

    def test_2(self):
        print("helou")
        pass


class NonTest:
    def test_2(self):
        print("tested2")
        pass
