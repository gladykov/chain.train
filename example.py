from lib.schema_definition import SchemaDefinition
from lib.stats import Stats

my_schema = SchemaDefinition("my_schema")

(
    my_schema.table("hello")
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
    my_schema.table("newest")
    .column("extra_colum", "great_type")
    .skip("fixme")
)
my_schema.close()

my_schema.environment_difference("production", "hesllo", "my_col", [("unique", False)])

pass
