from lib.schema_definition import SchemaDefinition
from lib.stats import Stats

schema_definition = SchemaDefinition("test_schema")

(
    schema_definition.table("test_table")
    .column("column_name", "text")
    .unique()
    .can_be_null()
    .collect_stat(Stats.DISTINCT)
    .expected_result(Stats.DISTINCT, qa=123, production=456)
    .allowed_values(["asdsa"])
    # .column("column_name_1", "text")
    .column("column_name_2", "int")
    .allowed_values(["Female", "Male"])
    .column("column_name_3", "bigint")
    .column("imnotthere", "bigint")
)

# (
#     schema_definition.table("test_table_2")
#     .column("column_name", "text")
#     .unique()
#     .can_be_null()
#     .collect_stat(Stats.DISTINCT)
#     .expected_result(Stats.DISTINCT, qa=123, production=456)
#     .allowed_values(["asdsa"])
#     .column("column_name_1", "text")
#     .column("column_name_2", "int")
#     .column("column_name_3", "bigint")
#     # .column("imnotthere", "bigint")
# )


schema_definition.close()

schema_definition.environment_difference("production", "test_table", "column_name_3", [("skip", "Bad test")])
pass
