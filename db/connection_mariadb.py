from db.connection import AbstractConnection
from tabulate import tabulate
import pandas as pd
from sqlalchemy import create_engine
import pymysql


class MariaDBConnection(AbstractConnection):

    def __init__(self, config):
        assert config["connector"] in ["mysql", "mariadb"]
        self.flavor = config["connector"]
        self.config = config["mariadb"]
        self.connection = self.connection()

    def connection(self):

        return pymysql.connect(
            user=self.config["user"],
            password=self.config["password"],
            host=self.config["host"],
            port=self.config["port"],
            # database=self.config["database"]
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

    def schema_exists(self, schema_name) -> bool:
        return schema_name in [row["Database"] for row in self.query("SHOW DATABASES").fetchall()]

    def save(self, schema_name, table_name, result, mode) -> None:

        overwrite = mode == "overwrite"

        # Only PyMySQL worked both for MariaDB and MySQL, when writing data from one schema to another, with open coursor
        # You can check if some bugs went away in the future
        connection = create_engine(f"{self.flavor}+pymysql://{self.config['user']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{schema_name}")

        df = pd.DataFrame(result.fetchall())
        df.columns = [i[0] for i in result.description]

        if_exists = "replace" if overwrite else "append"
        df.to_sql(table_name, con=connection, if_exists=if_exists)

    def close(self) -> None:
        self.connection.close()
