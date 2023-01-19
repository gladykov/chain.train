import importlib
import logging
from types import SimpleNamespace

from db.db_connection import Db
from helpers.parser import parser


def setup():
    setup = SimpleNamespace()
    setup.parser = parser()
    setup.db = Db(setup.parser.schema_name)
    setup.logger = logging.getLogger(__name__)
    setup.schema = importlib.import_module(
        f"schemas.{setup.parser.schema_name}"
    ).schema_definition.schema_for_environment(setup.parser.env)
    return setup
