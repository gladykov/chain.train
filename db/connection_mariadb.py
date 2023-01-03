from db.connection import AbstractConnection
from tabulate import tabulate
import mariadb
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
import pymysql.cursors

class MariaDBConnection(AbstractConnection):

    def __init__(self, name, flavor):
        self.flavor = flavor
        self.connection_details = {"user": "root", "password": "mysecret", "host": "127.0.0.1", "port":3306, "database":"menagerie"}
        self.connection = self.connection(flavor)

    def connection(self, flavor):
        assert flavor in ["mysql", "mariadb"]
        connection_flavor = mariadb if flavor == "mariadb" else mysql.connector

        return pymysql.connect(
        user=self.connection_details["user"],
        password=self.connection_details["password"],
        host=self.connection_details["host"],
        port=self.connection_details["port"],
        database=self.connection_details["database"]
    )

    def query(self, query) -> object:
        cursor = self.connection.cursor()
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
        return [row[f"Tables_in_{schema_name}"] for row in self.query(f"SHOW TABLES IN {schema_name}").fetchall()]

    def columns(self, schema_name, table_name) -> dict:
        query = f"DESCRIBE {schema_name}.{table_name}"
        return {col["Field"]: col["Type"] for col in self.query(query).fetchall()}

    def table_exists(self, schema_name, table_name) -> bool:
        return table_name in self.tables(schema_name)

    def schema_exists(self, schema_name) -> bool:
        return schema_name in [row["Database"] for row in self.query("SHOW DATABASES").fetchall()]

    def save(self, schema_name, table_name, result, mode) -> None:

        overwrite = mode == "overwrite"

        connection_flavor = "mariadb+pymysql" if self.flavor == "mariadb" else "mysql+pymysql"
        connection = create_engine(f"{connection_flavor}://root:mysecret@127.0.0.1:3306/{schema_name}")

        # Put it all to a data frame
        df = pd.DataFrame(result.fetchall())
        df.columns = [i[0] for i in result.description]
        # print([i[0] for i in result.description])

        print(df.dtypes)

        if_exists = "replace" if overwrite else "append"
        df.to_sql(table_name, con=connection, if_exists=if_exists)

    def close(self) -> None:
        self.connection.close()
