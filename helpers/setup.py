from helpers.parser import parser
from db.db_connection import Db
from types import SimpleNamespace
import logging

def setup():
    setup = SimpleNamespace()
    setup.parser = parser()
    setup.db = Db(setup.parser.schema_name)
    setup.logger = logging.getLogger(__name__)
    return setup