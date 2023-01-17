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
        # self.db.create_database("test_database")

        self.db.create_schema("TEST_SCHEMA")

        self.db.drop_table("TEST_SCHEMA", "TEST_TABLE_1")
        self.db.drop_table("TEST_SCHEMA", "TEST_TABLE_2")

        self.db.create_table(
            "TEST_SCHEMA",
            "TEST_TABLE_1",
            [("COLUMN_NAME", "text"), ("COLUMN_NAME_1", "text"), ("COLUMN_NAME_2", "int"), ("COLUMN_NAME_3", "bigint")]
        )

        self.db.insert("TEST_SCHEMA", "TEST_TABLE_1", [("row_1", "row_1_column_1", 12, 24), ("row_2", "row_2_column_1", 10, 12), ("row_3", "row_3_column_1", 3, 456)])

        self.db.create_table(
            "TEST_SCHEMA",
            "TEST_TABLE_2",
            [("COLUMN_NAME", "text"), ("COLUMN_NAME_1", "text"), ("COLUMN_NAME_2", "int"), ("COLUMN_NAME_3", "bigint"), ("COLUMN_NAME_4", "text")]
        )

        self.db.insert("TEST_SCHEMA", "TEST_TABLE_2", [("row_1", "row_1_column_1", 12, 24, None), ("row_2", "row_2_column_1", 10, 12, ""), ("row_3", "row_3_column_1", 3, 456, "arg"),("row_3", "row_3_column_1", 3, 456, "")])

GenerateData().generate_data()