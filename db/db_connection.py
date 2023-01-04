from lib.assets import config


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

    def __init__(self):
        super().__init__(config)

    def drop_table(self, schema, table):
        self.query(f"DROP TABLE IF EXISTS {schema}.{table}")

    def create_database(self, schema):
        self.query(f"CREATE DATABASE IF NOT EXISTS {schema}")

    def table_exists(self, schema_name, table_name):
        return table_name in self.tables(schema_name)

a = Db()
print(a.parse_result(a.query("SHOW DATABASES")))
a.create_database("mysql_mine")
print(a.parse_result(a.query("SHOW DATABASES")))
