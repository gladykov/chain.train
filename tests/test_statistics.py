import statistics

from pandas import DataFrame

from helpers.setup import setup as my_setup
from lib.stats import Stats
from tests.gather_statistics import STATISTICS_TABLE_NAME


EXPECTED_DATA_POINTS = 2 + 1  # How many data points we expect before doing comparison
DATA_POINTS_TO_LOOK_AT = 12 + 1  # How many data points in the past to look into
ACCEPTED_STD_DEV_DIFFERENCE = 3


class TestStatistics:
    def setup_class(self):
        setup = my_setup()
        self.env = setup.parser.env
        self.schema_name = setup.parser.schema_name
        self.schema = setup.schema
        self.logger = setup.logger
        self.db = setup.db
        query = "SELECT * FROM {schema_name}.{statistics_table_name}"
        self.statistics_data = self.db.rows(
            self.db.query(
                query.format(
                    schema_name=self.schema.statistics_schema_name,
                    statistics_table_name=STATISTICS_TABLE_NAME,
                )
            )
        )
        # Convert to DataFrame for filtering on multiple columns
        self.df = DataFrame([vars(row) for row in self.statistics_data])
        self.df.columns = (
            self.df.columns.str.upper()
        )  # Thank you Snowflake for forcing UPPERCASE

    def teardown_class(self):
        self.db.close()

    def filtered_stats(self, schema_name, table_name, column_name, stat_type):
        query = ('SCHEMA_NAME == "{schema_name}" & TABLE_NAME == "{table_name}" '
                 '& COLUMN_NAME == "{column_name}" & STAT_TYPE == "{stat_type}"')
        return self.df.query(
            query.format(
                schema_name=schema_name,
                table_name=table_name,
                column_name=column_name,
                stat_type=stat_type,
            )
        ).sort_values(by=["MEASURE_DATE"], ascending=False)[["VALUE"]]

    @staticmethod
    def enough_data_points(df):
        return len(df.index) >= EXPECTED_DATA_POINTS

    @staticmethod
    def current_past_values(df):
        return df.iloc[0].VALUE, list(df.iloc[1:DATA_POINTS_TO_LOOK_AT].VALUE)

    def test_standard_deviation(self):

        failures = []

        for table, column in self.schema.tables_columns_with_stats():
            for stat in column.gather_stats:
                df = self.filtered_stats(
                    self.schema_name, table.name, column.name, stat
                )

                if not self.enough_data_points(df):
                    continue

                current, past = self.current_past_values(df)
                standard_deviation = statistics.stdev(past)
                average = sum(past) / len(past)
                expected = average - ACCEPTED_STD_DEV_DIFFERENCE * standard_deviation

                if current < expected:
                    failures.append(
                        f"In table: {table.name} in column: {column.name} "
                        f"for stat {stat} expected std: {expected}. Actual: {current}"
                    )

    def test_stat_always_grow(self):

        failures = []

        for table, column in self.schema.tables_columns_with_stats():

            if not column.stat_always_grow:
                continue

            for stat in column.gather_stats:

                if stat not in [Stats.DISTINCT, Stats.TOTAL]:
                    continue

                df = self.filtered_stats(
                    self.schema_name, table.name, column.name, stat
                )

                if not len(df.index) >= 2:
                    continue

                current, past = self.current_past_values(df)
                previous = past[0]

                if current < previous:
                    failures.append(
                        f"In table: {table.name} in column: {column.name} "
                        f"for stat {stat} expected increase. "
                        f"Previous: {previous}. Actual: {current}"
                    )

    def test_latest_run_is_not_zero(self):

        failures = []

        for table, column in self.schema.tables_columns_with_stats():

            for stat in column.gather_stats:

                df = self.filtered_stats(
                    self.schema_name, table.name, column.name, stat
                )

                if len(df.index) == 0:
                    raise ValueError(
                        f"You got 0 recorded stats for {self.schema_name} {table.name} "
                        f"{column.name} {stat}"
                    )

                current, _ = self.current_past_values(df)

                if current == 0:
                    failures.append(
                        f"In table: {table.name} in column: {column.name} "
                        f"for latest stat {stat} got 0."
                    )
