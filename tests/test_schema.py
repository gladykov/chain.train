from expected_format_validators import ExpectedFormatValidators

from helpers.setup import setup as my_setup


class TestSchema:
    def setup_class(self):
        setup = my_setup()
        self.env = setup.parser.env
        self.schema_name = setup.parser.schema_name
        self.schema = setup.schema
        self.logger = setup.logger
        self.db = setup.db

        self.logger.info(
            f"Executing tests in env: {self.env} for schema: {self.schema_name}"
        )
        # Cache table names, not to fail some tests if table does not exist
        self.tables_in_db = sorted(self.db.tables(self.schema.name))
        # Cache schema from DB
        self.schema_in_db = {}
        for table_in_db in self.tables_in_db:
            self.schema_in_db[table_in_db] = self.db.columns(
                self.schema_name, table_in_db
            )

        self.schema.print_skip_info(self.logger)
        self.empty_tables = self.empty_tables(self)

    def teardown_class(self):
        self.db.close()

    def empty_tables(self):
        query = "SELECT 'EMPTY' FROM {schema_name}.{table} LIMIT 1"
        return [
            table
            for table in self.tables_in_db
            if self.db.empty_result(
                self.db.query(query.format(schema_name=self.schema_name, table=table))
            )
        ]

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
                failures.append(
                    f"For table: {table.name} missing columns in definition: "
                    f"{not_in_definition} and missing in DB: {not_in_db}"
                )

        assert not failures, f"{failures}"

    def test_column_types(self):
        """Check if columns in DB have correct type"""
        failures = []
        for table in self.schema.tables:
            for column in table.columns:

                if self.should_skip(table, column):
                    continue

                expected = column.type
                # Removing brackets and characters in brackets
                # 'int(12)' will become 'int`
                actual = next(
                    col_type
                    for name, col_type in self.schema_in_db[table.name].items()
                    if name == column.name
                ).split("(")[0]

                if expected != actual:
                    failures.append(
                        f"In table: {table.name} column: "
                        f"{column.name} expected type {expected}. Actual: {actual}"
                    )

        assert not failures, f"{failures}"

    def test_table_empty(self):
        assert not self.empty_tables, f"Empty tables in DB: {self.empty_tables}"

    def test_column_is_null_empty(self):

        query_null = ("SELECT {column_name} FROM {schema_name}.{table_name} "
                      "{row_limiter} {column_name} IS NOT NULL LIMIT 1")
        # To safely check other data types, cast them as string.
        # This could be costly operation if there are many empty strings in column.
        query_empty = (
            "SELECT {column_name} FROM {schema_name}.{table_name} "
            "{row_limiter} cast({column_name} as {string_cast}) <> '' LIMIT 1")

        failures = []
        for table in self.schema.tables:
            for column in table.columns:

                if self.should_skip(table, column):
                    continue

                if self.db.empty_result(
                    self.db.query(
                        query_null.format(
                            schema_name=self.schema.name,
                            table_name=table.name,
                            column_name=column.name,
                            row_limiter=table.get_row_limiter("AND"),
                        )
                    )
                ):
                    failures.append(
                        f"In table: {table.name} column: {column.name} "
                        f"has only null values"
                    )
                elif self.db.empty_result(
                    self.db.query(
                        query_empty.format(
                            schema_name=self.schema.name,
                            table_name=table.name,
                            column_name=column.name,
                            string_cast=self.db.string_cast,
                            row_limiter=table.get_row_limiter("AND"),
                        )
                    )
                ):
                    failures.append(
                        f"In table: {table.name} column: {column.name} "
                        f"has only empty strings"
                    )

        assert not failures, f"Empty columns: {failures}"

    def test_contains_null(self):
        query = (
            "SELECT {column_name} FROM {schema_name}.{table_name} "
            "{row_limiter} {column_name} IS NULL LIMIT 1"
        )

        failures = []
        for table in self.schema.tables:
            for column in table.columns:

                if column.null or self.should_skip(table, column):
                    continue

                if not self.db.empty_result(
                    self.db.query(
                        query.format(
                            schema_name=self.schema.name,
                            table_name=table.name,
                            column_name=column.name,
                            row_limiter=table.get_row_limiter("AND"),
                        )
                    )
                ):
                    failures.append(
                        f"In table: {table.name} column: {column.name} has null values"
                    )

        assert not failures, f"Found null values in columns: {failures}"

    def test_contains_empty(self):
        query = (
            "SELECT {column_name} FROM {schema_name}.{table_name} "
            "{row_limiter} cast({column_name} as {string_cast}) = '' LIMIT 1"
        )

        failures = []
        for table in self.schema.tables:
            for column in table.columns:

                if column.empty or self.should_skip(table, column):
                    continue

                if not self.db.empty_result(
                    self.db.query(
                        query.format(
                            schema_name=self.schema.name,
                            table_name=table.name,
                            column_name=column.name,
                            row_limiter=table.get_row_limiter("AND"),
                            string_cast=self.db.string_cast,
                        )
                    )
                ):
                    failures.append(
                        f"In table: {table.name} column: {column.name} has empty values"
                    )

        assert not failures, f"Found empty values in columns: {failures}"

    def test_unique(self):
        query = (
            "SELECT {column_name}, COUNT({column_name}) AS duplicates "
            "FROM {schema_name}.{table_name} {row_limiter} "
            "GROUP BY {column_name} HAVING COUNT({column_name}) > 1 LIMIT 1"
        )

        failures = []
        for table in self.schema.tables:
            for column in table.columns:

                if not column.unique or self.should_skip(table, column):
                    continue

                if not self.db.empty_result(
                    self.db.query(
                        query.format(
                            schema_name=self.schema.name,
                            table_name=table.name,
                            column_name=column.name,
                            row_limiter=table.get_row_limiter()
                            if table.row_limiter
                            else "",
                        )
                    )
                ):
                    failures.append(
                        f"In table: {table.name} column: {column.name} "
                        f"has duplicated values"
                    )

        assert not failures, f"Found duplicated data in unique columns {failures}"

    def test_allowed_values(self):
        def safe_list(unsafe_list):
            if None not in unsafe_list:
                return sorted(unsafe_list)

            return (
                sorted(list(filter(lambda item: item is not None, unsafe_list))),
                None,
            )

        query = ("SELECT {column_name} FROM {schema_name}.{table_name} "
                 "{row_limiter} GROUP BY {column_name}")

        failures = []

        for table in self.schema.tables:
            for column in table.columns:

                if not column.allowed_values or self.should_skip(table, column):
                    continue

                expected = column.allowed_values
                actual = [
                    getattr(row, column.name)
                    for row in self.db.rows(
                        self.db.query(
                            query.format(
                                schema_name=self.schema.name,
                                table_name=table.name,
                                column_name=column.name,
                                row_limiter=table.get_row_limiter()
                                if table.row_limiter
                                else "",
                            )
                        )
                    )
                ]

                # None / NULL will mess with sorting and comparison,
                # but we do not want to lose this info either
                expected = safe_list(expected)
                actual = safe_list(actual)

                if not expected == actual:
                    failures.append(
                        f"In table: {table.name} column: {column.name} "
                        f"expected values: {expected}. Actual: {actual}"
                    )

        assert not failures, f"Expected values differ from expected: {failures}"

    def test_min_value(self):
        # Beware. Snowflake knows better and converts column names to upper case
        query = ("SELECT min({column_name}) AS MIN_VALUE "
                 "FROM {schema_name}.{table_name} {row_limiter}")

        failures = []

        for table in self.schema.tables:
            for column in table.columns:

                if not column.min_value or self.should_skip(table, column):
                    continue

                expected = column.min_value
                actual = self.db.row(
                    self.db.query(
                        query.format(
                            schema_name=self.schema.name,
                            table_name=table.name,
                            column_name=column.name,
                            row_limiter=table.get_row_limiter()
                            if table.row_limiter
                            else "",
                        )
                    )
                ).MIN_VALUE

                if actual < expected:
                    failures.append(
                        f"In table: {table.name} column: {column.name} "
                        f"expected min value: {expected}. Actual: {actual}"
                    )

        assert not failures, f"Expected min values differ from expected: {failures}"

    def test_max_value(self):
        query = ("SELECT max({column_name}) AS MAX_VALUE "
                 "FROM {schema_name}.{table_name} {row_limiter}")

        failures = []

        for table in self.schema.tables:
            for column in table.columns:

                if not column.max_value or self.should_skip(table, column):
                    continue

                expected = column.max_value
                actual = self.db.row(
                    self.db.query(
                        query.format(
                            schema_name=self.schema.name,
                            table_name=table.name,
                            column_name=column.name,
                            row_limiter=table.get_row_limiter()
                            if table.row_limiter
                            else "",
                        )
                    )
                ).MAX_VALUE

                if actual > expected:
                    failures.append(
                        f"In table: {table.name} column: {column.name} "
                        f"expected max value: {expected}. Actual: {actual}"
                    )

        assert not failures, f"Expected max values differ from expected: {failures}"

    def test_expected_format(self):

        failures = []

        for table in self.schema.tables:
            for column in table.columns:

                if not column.expected_format or self.should_skip(table, column):
                    continue

                if not hasattr(ExpectedFormatValidators, column.expected_format):
                    failures.append(
                        f"In table: {table.name} column: {column.name} "
                        f"unrecognized validator {column.expected_format}."
                    )
                    continue

                try:
                    result = getattr(
                        self.db.sample(
                            self.schema.name,
                            table.name,
                            column.name,
                            table.get_row_limiter("AND"),
                        ),
                        column.name,
                    )
                except AttributeError as error:
                    failures.append(
                        f"In table: {table.name} column: {column.name} "
                        f"couldn't test expected {column.expected_format}. "
                        f"In sampled data we didn't find enough valid values. "
                        f"Error: {error} "
                    )
                    continue

                validator = getattr(ExpectedFormatValidators, column.expected_format)
                if not validator(result):
                    failures.append(
                        f"In table: {table.name} column: {column.name} "
                        f"expected {column.expected_format}. Actual: {result}"
                    )

        assert not failures, f"Some columns contain unexpected format: {failures}"
