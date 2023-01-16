from db.connection import AbstractConnection
from pyspark import SparkConf
from pyspark.sql import SparkSession


class SparkConnection(AbstractConnection):
    def __init__(self, config, _):
        self.connection = self.connection(config)

    def connection(self, config):
        spark_conf = SparkConf()
        for key, value in config["spark"].items():
            spark_conf.set(key, value)

        return SparkSession.builder.config(conf=spark_conf).getOrCreate()

    def query(self, query) -> object:
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

    def schema_exists(self, schema) -> bool:
        return any(database.name == schema for database in self.connection.catalog.listDatabases())

    def columns(self, schema, table) -> dict:
        return {col.name: col.dataType for col in self.connection.catalog.listColumns(table, schema)}

    def save(self, schema, table, result, mode) -> None:
        result.write.mode(mode).format("hive").saveAsTable(f"{schema}.{table}")

    def insert(self, schema_name, table_name, values):
        values = [values] if type(values) == tuple else values
        schema = self.connection.sql(f"SELECT * FROM {schema_name}.{table_name} LIMIT 1").schema
        df = self.connection.createDataFrame(values, schema)
        self.save(schema_name, table_name, df, "append")

    def sample(self, schema_name, table_name, column_name, row_delimiter):
        query = (
            "SELECT {column_name} FROM {schema_name}.{table_name} {row_delimiter} "
            "rand() <= {subset_percentage} AND {column_name} IS NOT NULL AND {column_name} <> '' distribute by rand() sort by rand() limit 1"
        )

        subset_percentage = (1 / 100) * self.subset_percentage

        result = self.query(
            query.format(
                schema_name=schema_name,
                table_name=table_name,
                column_name=column_name,
                subset_percentage=subset_percentage,
                row_delimiter=row_delimiter,
            )
        )

        return self.row(result)


    def close(self) -> None:
        if self.connection is not None:
            self.connection.stop()
