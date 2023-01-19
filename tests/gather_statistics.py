from datetime import datetime

from helpers.setup import setup as my_setup
from lib.stats import Stats


STATISTICS_TABLE_NAME = "statistics"


class GatherStatistics:
    def __init__(self):
        setup = my_setup()
        self.env = setup.parser.env
        self.schema_name = setup.parser.schema_name
        self.schema = setup.schema
        self.logger = setup.logger
        self.db = setup.db

    def prepare(self):
        string_types = {
            "mysql": "text",
            "mariadb": "text",
            "snowflake": "text",
            "spark": "string",
        }
        string_type = string_types[self.db.flavor]

        self.db.create_schema(self.schema.statistics_schema_name)
        self.db.create_table(
            self.schema.statistics_schema_name,
            STATISTICS_TABLE_NAME,
            [
                ("schema_name", f"{string_type}"),
                ("table_name", f"{string_type}"),
                ("column_name", f"{string_type}"),
                ("stat_type", f"{string_type}"),
                ("value", "bigint"),
                ("measure_date", "timestamp"),
            ],
        )

    def gather_statistics(self):
        query = (
            "SELECT {distinct} {column_name} "
            "FROM {schema_name}.{table_name} {row_limiter}"
        )

        gathered_statistics = []

        for table, column in self.schema.tables_columns_with_stats():
            for stat in column.gather_stats:
                distinct = (
                    "DISTINCT"
                    if stat in [Stats.DISTINCT, Stats.DISTINCT_IN_RANGE]
                    else ""
                )

                # To get partial stats you need to define row_limiter for your table,
                # and type of stat must be one of TOTAL_IN_RANGE or DISTINCT_IN_RANGE
                if (
                    stat in [Stats.TOTAL_IN_RANGE, Stats.DISTINCT_IN_RANGE]
                    and not table.row_limiter
                ):
                    continue

                row_limiter = (
                    f"WHERE {table.row_limiter}"
                    if table.row_limiter
                    and stat in [Stats.TOTAL_IN_RANGE, Stats.DISTINCT_IN_RANGE]
                    else ""
                )
                value = self.db.count(
                    self.db.query(
                        query.format(
                            distinct=distinct,
                            column_name=column.name,
                            schema_name=self.schema.name,
                            table_name=table.name,
                            row_limiter=row_limiter,
                        )
                    )
                )
                gathered_statistics.append(
                    (
                        self.schema_name,
                        table.name,
                        column.name,
                        str(stat),
                        value,
                        datetime.now(),
                    )
                )

        self.db.insert(
            self.schema.statistics_schema_name,
            STATISTICS_TABLE_NAME,
            gathered_statistics,
        )

    def main(self):
        self.prepare()
        self.gather_statistics()


if __name__ == "__main__":
    GatherStatistics().main()
