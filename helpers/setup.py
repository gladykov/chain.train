from helpers.parser import parser
from db.db_connection import Db
from types import SimpleNamespace


def setup():
    setup = SimpleNamespace()
    setup.parser = parser()
    # setup.db = Db(setup.parser.schema_name)

    return setup