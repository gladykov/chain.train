from helpers.parser import parser
from db.db_connection import Db
from types import SimpleNamespace
import logging
import importlib


def setup():
    setup = SimpleNamespace()
    setup.parser = parser()
    setup.db = Db(setup.parser.schema_name)
    setup.logger = logging.getLogger(__name__)
    setup.schema = importlib.import_module(
            f"schemas.{setup.parser.schema_name}"
        ).schema_definition.schema_for_environment(setup.parser.env)
    return setup