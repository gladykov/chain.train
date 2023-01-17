from db.connection import AbstractConnection
from tabulate import tabulate
import pandas as pd
from sqlalchemy import create_engine
import pymysql
from pymysql.cursors import DictCursor


class MariaDBConnection(AbstractConnection):

    string_cast = "char"

    def __init__(self, config, schema_name):
        assert config["connector"] in ["mysql", "mariadb"]
        self.flavor = config["connector"]
        self.subset_percentage = config["sample_subset_percentage"]
        self.config = config["mariadb"]
        self.schema_name = schema_name
        self.connection = self.connection()

    def connection(self):

        return pymysql.connect(
            user=self.config["user"],
            password=self.config["password"],
            host=self.config["host"],
            port=self.config["port"],
            # database=self.schema_name
        )

    def query(self, query) -> object:
        cursor = self.connection.cursor(DictCursor)
        cursor.execute(query)
        return cursor

    def empty_result(self, result) -> bool:
        return result.rowcount == 0

    def count(self, result) -> int:
        return result.rowcount

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
            list(row.values())[0]
            for row in self.query(f"SHOW TABLES IN {schema_name}").fetchall()
        ]

    def columns(self, schema_name, table_name) -> dict:
        query = f"DESCRIBE {schema_name}.{table_name}"
        return {col["Field"]: col["Type"] for col in self.query(query).fetchall()}

    def schema_exists(self, schema_name) -> bool:
        return schema_name in [
            row["Database"] for row in self.query("SHOW DATABASES").fetchall()
        ]

    def sample(self, schema_name, table_name, column_name, row_delimiter):
        query = (
            "SELECT {column_name} FROM {schema_name}.{table_name} {row_delimiter} "
            "rand() <= {subset_percentage} AND {column_name} IS NOT NULL AND {column_name} <> '' order by rand() limit 1"
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
        values = [values] if type(values) == tuple else values

        for row in values:
            values_string = ("%s, " * len(row)).rstrip(", ")
            query = f"INSERT INTO {schema_name}.{table_name} VALUES ({values_string})"
            self.connection.cursor().execute(query, row)

        self.connection.commit()

    def save(self, schema_name, table_name, result, mode) -> None:

        overwrite = mode == "overwrite"

        # Only PyMySQL worked both for MariaDB and MySQL, when writing data from one schema to another, with open coursor
        # You can check if some bugs went away in the future
        connection = create_engine(
            f"{self.flavor}+pymysql://{self.config['user']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{schema_name}"
        )

        df = pd.DataFrame(result.fetchall())
        df.columns = [i[0] for i in result.description]

        if_exists = "replace" if overwrite else "append"
        df.to_sql(table_name, con=connection, if_exists=if_exists)

    def create_table(self, schema, table, columns):
        columns_string = ", ".join([f"`{column_name}` {column_type}" for column_name, column_type in columns])
        query = (
            f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({columns_string})"
        )
        self.query(query)

    def close(self) -> None:
        self.connection.close()
