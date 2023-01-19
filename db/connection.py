from abc import ABC, abstractmethod
from types import SimpleNamespace


class AbstractConnection(ABC):
    @abstractmethod
    def connection(self, name):
        pass

    @abstractmethod
    def query(self, query) -> object:
        pass

    @abstractmethod
    def empty_result(self, result) -> bool:
        pass

    @abstractmethod
    def count(self, result) -> int:
        pass

    @abstractmethod
    def row(self, result) -> object:
        pass

    @abstractmethod
    def rows(self, result) -> list:
        pass

    @abstractmethod
    def parse_result(self, result) -> str:
        pass

    @abstractmethod
    def tables(self, schema_name) -> list:
        pass

    @abstractmethod
    def columns(self, schema_name, table_name) -> dict:
        pass

    @abstractmethod
    def schema_exists(self, schema_name) -> bool:
        pass

    @abstractmethod
    def save(self, schema_name, table_name, result, mode) -> None:
        """pass results object
        mode: overwrite, append
        """

    @abstractmethod
    def insert(self, schema_name, table_name, values) -> None:
        """Values - expects tuple or list of tuples"""

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def create_table(self, schema_name, table_name, columns) -> None:
        pass

    @abstractmethod
    def sample(self, schema_name, table_name, column_name, row_limiter) -> object:
        """Take one sample. Limit initial data pulled for shuffling to 1%. Take only not null and not empty strings.
        Sample methods differ depending on engine used
        """

    @staticmethod
    def _objectify_row(row):
        objectified_row = SimpleNamespace()
        for key, value in row.items():
            setattr(objectified_row, key, value)
        return objectified_row
