class Schema:
    def __init__(self, name):
        self.name = name
        self.tables = []
        self.statistics_schema_name = f"{name}_statistics"
        self.regression_schema_name = f"{name}_regression"

    def table_pointer(self, name):
        return next(
            index for index, table in enumerate(self.tables) if table.name == name
        )

    def table_names(self):
        return [table.name for table in self.tables]

