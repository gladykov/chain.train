from db.connection import AbstractConnection
from snowflake import connector
import os
from snowflake.connector import DictCursor
from types import SimpleNamespace
from tabulate import tabulate


class SnowflakeConnection(AbstractConnection):

    def __init__(self, name):
        self.connection = self.connection()

    @staticmethod
    def _objectify_row(row):
        objectified_row = SimpleNamespace()
        for key, value in row.items():
            setattr(objectified_row, key, value)
        return objectified_row

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

    def row(self, result) -> list:
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
        pass

    def columns(self, schema_name, table_name) -> dict:
        pass

    def table_exists(self, schema_name, table_name) -> bool:
        pass

    def schema_exists(self, schema_name) -> bool:
        pass

    def save(self, schema_name, table_name, result, mode) -> None:
        pass

    def close(self) -> None:
        self.connection.close()
