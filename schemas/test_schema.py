from lib.schema_definition import SchemaDefinition
from lib.stats import Stats

schema_definition = SchemaDefinition("TEST_SCHEMA")

(
    schema_definition.table("TEST_TABLE_1")
    .column("COLUMN_NAME", "text")
    .unique()
    .can_be_null()
    .collect_stat(Stats.DISTINCT)
    .collect_stat(Stats.TOTAL_IN_RANGE)
    .expected_result(Stats.DISTINCT, qa=123, production=456)
    # .allowed_values(["asdsa"])
    .column("COLUMN_NAME_1", "text")
    .column("COLUMN_NAME_2", "int")
    .collect_stat(Stats.TOTAL_IN_RANGE)
    .column("COLUMN_NAME_3", "bigint")
    .column("NOT_THERE", "bigint")
)

(
    schema_definition.table("TEST_TABLE_2") #.row_limiter("column_name_3 > 200")
    .column("COLUMN_NAME", "text")
    .unique()
    .can_be_null()
    .collect_stat(Stats.DISTINCT)
    .expected_result(Stats.DISTINCT, qa=123, production=456)
    .allowed_values(["row_1", "row_2", "row_3"])
    .column("COLUMN_NAME_1", "text").unique()
    .column("COLUMN_NAME_2", "int").min_value(5)
    .column("COLUMN_NAME_3", "bigint").max_value(200)
    .column("COLUMN_NAME_4", "text").allowed_values(["arg", "", None])
)


schema_definition.close()

schema_definition.environment_difference("production", "TEST_TABLE_1", "COLUMN_NAME_3", [("skip", "Bad test")])
pass
