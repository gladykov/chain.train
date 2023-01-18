class Schema:
    def __init__(self, name):
        self.name = name
        self.tables = []
        self.statistics_schema_name = f"{name}_statistics"
        self.regression_schema_name = f"{name}_REGRESSION"

    def table_pointer(self, name):
        return next(
            index for index, table in enumerate(self.tables) if table.name == name
        )

    def table_names(self):
        return [table.name for table in self.tables]

    def print_skip_info(self, logger):
        for table in self.tables:
            table.print_skipped_columns(logger)

    def tables_columns(self):
        for table in self.tables:
            for column in table.columns:
                yield table, column

    def tables_columns_with_stats(self):
        return [(table, column) for table, column in self.tables_columns() if column.gather_stats]

    def tables_columns_with_expected_result(self):
        return [(table, column) for table, column in self.tables_columns() if column.expected_results]

