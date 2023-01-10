from helpers.setup import setup as my_setup
import importlib


class TestSchema:

    @classmethod
    def setup_class(cls):
        setup = my_setup()
        cls.env = setup.parser.env
        cls.schema_name = setup.parser.schema_name
        cls.schema = importlib.import_module(f"schemas.{cls.schema_name}").schema_definition.schema_for_environment(cls.env)
        cls.logger = setup.logger
        cls.db = setup.db
        cls.logger.info(f"Executing tests in env: {cls.env} for schema: {cls.schema_name}")
        # Cache table names, not to fail some tests if table does not exist
        cls.tables_in_db = sorted(cls.db.tables(cls.schema.name))
        # Cache schema from DB
        cls.schema_in_db = {}
        for table_in_db in cls.tables_in_db:
            cls.schema_in_db[table_in_db] = cls.db.columns(cls.schema_name, table_in_db)

        cls.schema.print_skip_info(cls.logger)
        cls.empty_tables = cls.empty_tables(cls)

    def empty_tables(self):
        query = "SELECT 'EMPTY' FROM {schema_name}.{table} LIMIT 1"
        return [table for table in self.tables_in_db if self.db.empty_result(self.db.query(query.format(schema_name=self.schema_name, table=table)))]

    def should_skip(self, table, column):
        if table.name not in self.tables_in_db:
            return True

        if table.name in self.empty_tables:
            return True

        if column.name not in self.schema_in_db[table.name].keys():
            return True

        if column.skip:
            return True

        return False

    def test_table_names(self):
        """Check if all tables are in DB"""
        expected = sorted(self.schema.table_names())
        actual = self.tables_in_db
        assert expected == actual, "Tables in DB are different than expected"

    def test_column_names(self):
        """Check if all columns are in DB"""
        failures = []
        for table in self.schema.tables:
            expected = sorted(table.column_names())
            actual = sorted(self.db.columns(self.schema.name, table.name).keys())
            if expected != actual:
                not_in_db = list(set(expected) - set(actual))
                not_in_definition = list(set(actual) - set(expected))
                failures.append(f"For table {table.name} missing columns in definition: {not_in_definition} and missing in DB: {not_in_db}")

        assert not failures, f"{failures}"

    def test_column_types(self):
        """Check if columns in DB have correct type"""
        failures = []
        for table in self.schema.tables:
            for column in table.columns:

                if self.should_skip(table, column):
                    continue

                expected = column.type
                # Removing brackets and characters in brackets, so 'int(12)' will become 'int`
                actual = next(col_type for name, col_type in self.schema_in_db[table.name].items() if name == column.name).split("(")[0]

                if expected != actual:
                    failures.append(
                        f"For table {table.name} for column {column.name} expected type {expected}. Got: {actual}")

        assert not failures, f"{failures}"

    def test_table_empty(self):
        assert not self.empty_tables, f"Empty tables in DB: {self.empty_tables}"

    def test_column_null_empty(self):
        query_null = "SELECT {column_name} FROM {schema_name}.{table_name} WHERE {column_name} IS NOT NULL LIMIT 1"
        query_empty = "SELECT {column_name} FROM {schema_name}.{table_name} WHERE {column_name} <> '' LIMIT 1"

        failures = []
        for table in self.schema.tables:
            for column in table.columns:

                if self.should_skip(table, column):
                    print("skipping " + column.name)
                    continue

                if self.db.empty_result(self.db.query(query_null.format(column_name = column.name, schema_name = self.schema.name, table_name = table.name))):
                    failures.append(f"Column {column.name} in {table.name} has only null values")
                elif self.db.empty_result(self.db.query(query_empty.format(column_name = column.name, schema_name = self.schema.name, table_name = table.name))):
                    failures.append(f"Column {column.name} in {table.name} has only empty strings")

        assert not failures, f"Empty columns: {failures}"
