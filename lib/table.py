class Table:
    def __init__(self, name):
        self.name = name
        self.columns = []
        self.unique_columns_group = []

    def column_pointer(self, name):
        return next(
            index for index, column in enumerate(self.columns) if column.name == name)

    def column_names(self):
        return [column.name for column in self.columns]

    def column_types(self):
        return [column.name for column in self.columns]