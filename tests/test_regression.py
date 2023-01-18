from helpers.setup import setup as my_setup
from lib.stats import Stats


class TestRegression:

    @classmethod
    def setup_class(cls):
        setup = my_setup()
        cls.env = setup.parser.env
        cls.schema_name = setup.schema.regression_schema_name
        cls.schema = setup.schema
        cls.logger = setup.logger
        cls.db = setup.db

    def test_expected_results(self):
        # When you process fixed set of data, you expect same counts
        query = "SELECT {distinct} {column_name} FROM {schema_name}.{table_name}"

        failures = []

        for table, column in self.schema.tables_columns_with_expected_result():
            for expected_result in column.expected_results_for_environment(self.env):

                distinct = "DISTINCT" if expected_result.stat == Stats.DISTINCT else ""
                count = self.db.count(self.db.query(query.format(
                    distinct=distinct,
                    column_name=column.name,
                    schema_name=self.schema_name,
                    table_name=table.name
                )))

                if not count == expected_result.expected_count:
                    failures.append(f"In table: {table.name} in column: {column.name} expected count for stat {expected_result.stat} is: {expected_result.expected_count}. Actual: {count}")

        assert not failures, failures
