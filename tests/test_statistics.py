from helpers.setup import setup as my_setup
from tests.gather_statistics import STATISTICS_TABLE_NAME
from pandas import DataFrame
import statistics
from lib.stats import Stats

EXPECTED_DATA_POINTS = 2 + 1  # How many data points we expect before doing comparison
DATA_POINTS_TO_LOOK_AT = 12 + 1  # How many data points in the past to look into
ACCEPTED_STD_DEV_DIFFERENCE = 3


class TestStatistics:

    @classmethod
    def setup_class(cls):
        setup = my_setup()
        cls.env = setup.parser.env
        cls.schema_name = setup.parser.schema_name
        cls.schema = setup.schema
        cls.logger = setup.logger
        cls.db = setup.db
        query = "SELECT * FROM {schema_name}.{statistics_table_name}"
        cls.statistics_data = cls.db.rows(cls.db.query(query.format(
            schema_name = cls.schema.statistics_schema_name,
            statistics_table_name=STATISTICS_TABLE_NAME
        )))
        # Convert to DataFrame for filtering on multiple columns
        cls.df = DataFrame([vars(row) for row in cls.statistics_data])

    def filtered_stats(self, schema_name, table_name, column_name, stat_type):
        query = 'schema_name == "{schema_name}" & table_name == "{table_name}" & column_name == "{column_name}" & stat_type == "{stat_type}"'
        return self.df.query(
            query.format(
                schema_name=schema_name,
                table_name=table_name,
                column_name=column_name,
                stat_type=stat_type
            )
        ).sort_values(by=['measure_date'], ascending=False)[["value"]]

    @staticmethod
    def enough_data_points(df):
        return len(df.index) >= EXPECTED_DATA_POINTS

    @staticmethod
    def current_past_values(df):
        return df.iloc[0].value, list(df.iloc[1:DATA_POINTS_TO_LOOK_AT].value)

    def test_standard_deviation(self):

        failures = []

        for table, column in self.schema.tables_columns_with_stats():
            for stat in column.gather_stats:
                df = self.filtered_stats(self.schema_name, table.name, column.name, stat)

                if not self.enough_data_points(df):
                    continue

                current, past = self.current_past_values(df)
                standard_deviation = statistics.stdev(past)
                average = sum(past) / len(past)
                expected = average - ACCEPTED_STD_DEV_DIFFERENCE * standard_deviation

                if current < expected:
                    failures.append(f"In table: {table.name} in column: {column.name} for stat {stat} expected std: {expected}. Actual: {current}")

    def test_stat_always_grow(self):

        failures = []

        for table, column in self.schema.tables_columns_with_stats():

            if not column.stat_always_grow:
                continue

            for stat in column.gather_stats:

                if stat not in [Stats.DISTINCT, Stats.TOTAL]:
                    continue

                df = self.filtered_stats(self.schema_name, table.name, column.name, stat)

                if not len(df.index) >= 2:
                    continue

                current, past = self.current_past_values(df)
                previous = past[0]

                if current < previous:
                    failures.append(f"In table: {table.name} in column: {column.name} for stat {stat} expected increase. Previous: {previous}. Actual: {current}")

    def test_latest_run_is_not_zero(self):

        failures = []

        for table, column in self.schema.tables_columns_with_stats():

            for stat in column.gather_stats:

                df = self.filtered_stats(self.schema_name, table.name, column.name, stat)

                if len(df.index) == 0:
                    raise ValueError(f"You got 0 recorded stats for {self.schema_name} {table.name} {column.name} {stat}")

                current, _ = self.current_past_values(df)

                if current == 0:
                    failures.append(f"In table: {table.name} in column: {column.name} for latest stat {stat} got 0.")
