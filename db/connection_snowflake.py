from db.connection import AbstractConnection
from snowflake import connector
from snowflake.connector import DictCursor
from snowflake.connector.pandas_tools import write_pandas
from types import SimpleNamespace
from tabulate import tabulate
import json


class SnowflakeConnection(AbstractConnection):

    def __init__(self, name):
        self.connection = self.connection()

    def connection(self):
        return connector.connect(
            user="GLADYKOV",
            account="ca29959.sa-east-1.aws",
            password="Caparros1!",
            database="SNOWFLAKE_SAMPLE_DATA",
            role="ACCOUNTADMIN",
            schema="TPCDS_SF100TCL",
            warehouse="COMPUTE_WH",
            # authenticator="externalbrowser",
        )

    def query(self, query) -> object:
        return self.connection.cursor(DictCursor).execute(query)

    def empty_result(self, result) -> bool:
        return result.rowcount() is None

    def count(self, result) -> int:
        count = result.rowcount()
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
        return [row["name"] for row in self.query(f"SHOW TABLES IN {schema_name}").fetchall()]

    def columns(self, schema_name, table_name) -> dict:
        query = f"DESCRIBE TABLE {schema_name}.{table_name}"
        return {col["name"]: col["type"] for col in self.query(query).fetchall()}

    def table_exists(self, schema_name, table_name) -> bool:
        return table_name in self.tables(schema_name)

    def schema_exists(self, schema_name) -> bool:
        return schema_name in [row["name"] for row in self.query("SHOW DATABASES").fetchall()]

    def save(self, schema_name, table_name, result, mode) -> None:

        # When doing write operation to different database / schema you may need new connection
        database = "REGRESSION_DATABASE"

        connection = connector.connect(
            user="GLADYKOV",
            account="ca29959.sa-east-1.aws",
            password="Caparros1!",
            database=database,
            role="ACCOUNTADMIN",
            schema=schema_name,
            warehouse="COMPUTE_WH",
            # authenticator="externalbrowser",
        )

        overwrite = mode == "overwrite"
        df = result.fetch_pandas_all()
        write_pandas(connection, df, table_name=table_name, database=database, schema=schema_name, auto_create_table=True, overwrite=overwrite)
        connection.close()

    def close(self) -> None:
        self.connection.close()
