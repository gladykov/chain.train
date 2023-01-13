from helpers.setup import setup as my_setup


class GenerateData:

    def __init__(self):
        setup = my_setup()
        self.env = setup.parser.env
        self.schema_name = setup.parser.schema_name
        self.logger = setup.logger
        self.db = setup.db
        self.logger.info(f"Executing tests in env: {self.env} for schema: {self.schema_name}")

    def generate_data(self):
        self.db.create_database("test_schema")

        self.db.drop_table("test_schema", "test_table_1")
        self.db.drop_table("test_schema", "test_table_2")

        self.db.create_table(
            "test_schema",
            "test_table_1",
            [("column_name", "text"), ("column_name_1", "text"), ("column_name_2", "int"), ("column_name_3", "bigint")]
        )

        self.db.insert("test_schema", "test_table_1", ("row_1", "row_1_column_1", 12, 24))
        self.db.insert("test_schema", "test_table_1", ("row_2", "row_2_column_1", 10, 12))
        self.db.insert("test_schema", "test_table_1", ("row_3", "row_3_column_1", 3, 456))

        self.db.create_table(
            "test_schema",
            "test_table_2",
            [("column_name", "text"), ("column_name_1", "text"), ("column_name_2", "int"), ("column_name_3", "bigint"), ("column_name_4", "text")]
        )

        self.db.insert("test_schema", "test_table_2", ("row_1", "row_1_column_1", 12, 24, None))
        self.db.insert("test_schema", "test_table_2", ("row_2", "row_2_column_1", 10, 12, ""))
        self.db.insert("test_schema", "test_table_2", ("row_3", "row_3_column_1", 3, 456, "arg"))
        self.db.insert("test_schema", "test_table_2", ("row_3", "row_3_column_1", 3, 456, ""))


GenerateData().generate_data()