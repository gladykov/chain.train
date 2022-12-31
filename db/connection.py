from abc import ABC, abstractmethod


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
    def table_exists(self, schema_name, table_name) -> bool:
        pass

    @abstractmethod
    def schema_exists(self, schema_name) -> bool:
        pass

    @abstractmethod
    def save(self, schema_name, table_name, result, mode) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass
