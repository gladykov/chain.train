class Table:
    def __init__(self, name):
        self.name = name
        self.columns = []
        self.row_limiter = ""
        self.unique_columns_group = []

    def column_pointer(self, name):
        return next(
            index for index, column in enumerate(self.columns) if column.name == name)

    def column_names(self):
        return [column.name for column in self.columns]

    def column_types(self):
        return [column.name for column in self.columns]

    def print_skipped_columns(self, logger):
        for column in self.columns:
            if column.skip:
                logger.info(f"Skipping testing column {column.name} in {self.name} because of {column.skip}")

    def get_row_limiter(self, suffix=""):
        if not self.row_limiter:
            return "WHERE"

        return f"WHERE {self.row_limiter} {suffix}"
