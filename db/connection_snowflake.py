import logging

from snowflake import connector
from snowflake.connector import DictCursor
from snowflake.connector.pandas_tools import write_pandas
from tabulate import tabulate

from db.connection import AbstractConnection


logging.getLogger("snowflake.connector").setLevel(logging.WARNING)


class SnowflakeConnection(AbstractConnection):

    string_cast = "string"

    def __init__(self, config, schema_name):
        self.flavor = config["connector"]
        self.config = config["snowflake"]
        self.schema_name = schema_name
        self.connection = self.connection()
        self.subset_percentage = config["sample_subset_percentage"]

    def connection(self):

        return connector.connect(
            user=self.config["user"],
            account=self.config["account"],
            password=self.config["password"],
            database=self.config["database"],
            role=self.config["role"],
            schema=self.schema_name,
            warehouse=self.config["warehouse"],
            # authenticator="externalbrowser",
        )

    def query(self, query) -> object:
        return self.connection.cursor(DictCursor).execute(query)

    def empty_result(self, result) -> bool:
        return result.rowcount == 0

    def count(self, result) -> int:
        count = result.rowcount
        return count if count else 0

    def row(self, result) -> object:
        return self._objectify_row(result.fetchone())

    def rows(self, result) -> list:
        rows = result.fetchall()
        return [self._objectify_row(row) for row in rows]

    def parse_result(self, result) -> str:
        return tabulate(
            result.fetchall(),
            headers="keys",
            tablefmt="fancy_grid",
        )

    def tables(self, schema_name) -> list:
        return [
            row["name"]
            for row in self.query(f"SHOW TABLES IN {schema_name}").fetchall()
        ]

    def columns(self, schema_name, table_name) -> dict:
        query = f"DESCRIBE TABLE {schema_name}.{table_name}"
        return {col["name"]: col["type"] for col in self.query(query).fetchall()}

    def schema_exists(self, schema_name) -> bool:
        return schema_name in [
            row["name"] for row in self.query("SHOW DATABASES").fetchall()
        ]

    def save(self, schema_name, table_name, result, mode) -> None:

        # When doing write operation to different database / schema
        # you may need new connection
        database = "REGRESSION_DATABASE"

        connection = connector.connect(
            user=self.config["user"],
            account=self.config["account"],
            password=self.config["password"],
            database=database,
            role=self.config["role"],
            schema=schema_name,
            warehouse=self.config["warehouse"],
            # authenticator="externalbrowser",
        )

        overwrite = mode == "overwrite"
        df = result.fetch_pandas_all()
        write_pandas(
            connection,
            df,
            table_name=table_name,
            database=database,
            schema=schema_name,
            auto_create_table=True,
            overwrite=overwrite,
        )
        connection.close()

    def sample(self, schema_name, table_name, column_name, row_delimiter):
        query = (
            "SELECT {column_name} FROM {schema_name}.{table_name} "
            "WHERE {row_delimiter} "
            "rand() <= {subset_percentage} AND {column_name} IS NOT NULL AND "
            "{column_name} <> '' order by rand() limit 1"
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

    def insert(self, schema_name, table_name, values):
        values = [values] if isinstance(values, tuple) else values

        for row in values:
            values_string = ("%s, " * len(row)).rstrip(", ")
            query = f"INSERT INTO {schema_name}.{table_name} VALUES ({values_string})"
            self.connection.cursor().execute(query, row)

        self.connection.commit()

    def create_table(self, schema_name, table_name, columns):
        columns_string = ", ".join(
            [f"{column_name} {column_type}" for column_name, column_type in columns]
        )
        query = (
            f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({columns_string})"
        )
        self.query(query)

    def create_database(self, database):
        # For Snowflake Database is higher entity and contains many schemas
        self.query(f"CREATE DATABASE IF NOT EXISTS {database}")

    def close(self) -> None:
        self.connection.close()
