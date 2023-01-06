from lib.schema_definition import SchemaDefinition
from lib.stats import Stats

schema_definition = SchemaDefinition("my_schema")

(
    schema_definition.table("hello")
    .column("my_col", "int")
    .unique()
    .can_be_null()
    .collect_stat(Stats.DISTINCT)
    .expected_result(Stats.DISTINCT, qa=123, production=456)
    .allowed_values(["asdsa"])
    .column("very", "type")
    .max_value(10)
    .min_value(8)
    .expected_format("date")
)

(
    schema_definition.table("newest")
    .column("extra_colum", "great_type")
    .skip("fixme")
)
schema_definition.close()

schema_definition.environment_difference("production", "hello", "my_col", [("unique", False)])


pass