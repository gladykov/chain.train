from helpers.config import config


config = config()
connector = config["connector"]

if connector == "spark":
    from db.connection_spark import SparkConnection

    BaseClass = SparkConnection
elif connector == "snowflake":
    from db.connection_snowflake import SnowflakeConnection

    BaseClass = SnowflakeConnection
elif connector in ["mariadb", "mysql"]:
    from db.connection_mariadb import MariaDBConnection

    BaseClass = MariaDBConnection
else:
    raise AttributeError("Unrecognized connector, cannot initialize connection")


class Db(BaseClass):
    def __init__(self, schema_name):
        super().__init__(config, schema_name)

    def drop_table(self, schema_name, table_name):
        self.query(f"DROP TABLE IF EXISTS {schema_name}.{table_name}")

    def create_schema(self, schema_name):
        self.query(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    def table_exists(self, schema_name, table_name):
        return table_name in self.tables(schema_name)
