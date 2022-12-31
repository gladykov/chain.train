from db.connection import AbstractConnection
from pyspark.sql import SparkSession


class SparkConnection(AbstractConnection):
    def __init__(self, name):
        self.connection = self.connection(name)

    def connection(self, name):
        return SparkSession.builder.appName(name).getOrCreate()

    def query(self, query):
        return self.connection.sql(query)

    def empty_result(self, result) -> bool:
        return result.count() == 0

    def count(self, result) -> int:
        return result.count()

    def row(self, result) -> object:
        return result.take(1)[0]

    def rows(self, result) -> list:
        return result.collect()

    def parse_result(self, result) -> str:
        return result._jdf.showString(1000, 0, False)

    def tables(self, schema) -> list:
        return [table.name for table in self.connection.catalog.listTables(schema)]

    def table_exists(self, schema, table) -> bool:
        return any(existing_table.name == table for existing_table in self.connection.catalog.listTables(schema))

    def schema_exists(self, schema) -> bool:
        return any(database.name == schema for database in self.connection.catalog.listDatabases())

    def columns(self, schema, table) -> dict:
        return {col.name: col.dataType for col in self.connection.catalog.listColumns(table, schema)}

    def save(self, schema, table, result, mode) -> None:
        result.write.mode(mode).saveAsTable(f"{schema}.{table}")

    def close(self):
        if self.connection is not None:
            self.connection.stop()
