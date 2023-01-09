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

    def drop_table(self, schema, table):
        self.query(f"DROP TABLE IF EXISTS {schema}.{table}")

    def create_database(self, schema):
        self.query(f"CREATE DATABASE IF NOT EXISTS {schema}")

    def create_table(self, schema, table, columns):
        columns_string = ", ".join([f"`{column_name}` {column_type}" for column_name, column_type in columns])
        query = (
            f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({columns_string})"
        )
        self.query(query)

    def insert(self, schema, table, values):
        values_string = ", ".join([f"'{value}'" for value in values])
        query = f"INSERT INTO {schema}.{table} VALUES ({values_string})"
        self.query(query)

    def table_exists(self, schema_name, table_name):
        return table_name in self.tables(schema_name)
