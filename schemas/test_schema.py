from lib.schema_definition import SchemaDefinition
from lib.stats import Stats

schema_definition = SchemaDefinition("test_schema")

(
    schema_definition.table("test_table_1")
    .column("column_name", "texta")
    .unique()
    .can_be_null()
    .collect_stat(Stats.DISTINCT)
    .expected_result(Stats.DISTINCT, qa=123, production=456)
    # .allowed_values(["asdsa"])
    .column("column_name_1", "text")
    .column("column_name_2", "int")
    .column("column_name_3", "bigint")
    .column("not_there", "bigint")
)

(
    schema_definition.table("test_table_2") #.row_limiter("column_name_3 > 200")
    .column("column_name", "text")
    .unique()
    .can_be_null()
    .collect_stat(Stats.DISTINCT)
    .expected_result(Stats.DISTINCT, qa=123, production=456)
    .allowed_values(["row_1", "row_2", "row_3"])
    .column("column_name_1", "text").unique()
    .column("column_name_2", "int").min_value(5)
    .column("column_name_3", "bigint").max_value(200)
    .column("column_name_4", "text").allowed_values(["arg", ""])
)


schema_definition.close()

schema_definition.environment_difference("production", "test_table_1", "column_name_3", [("skip", "Bad test")])
pass
