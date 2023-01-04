from db.connection_snowflake import SnowflakeConnection
from db.connection_mariadb import MariaDBConnection

a = MariaDBConnection("name", "mysql")


# r = a.query("show tables")
#
# print(a.parse_result(r))
# print("---")
# d = a.query("select * from CALL_CENTER limit 10")
# print(a.parse_result(d))

print(a.query("SHOW DATABASES").fetchall())
print(a.query("CREATE DATABASE IF NOT EXISTS menagerie"))
print(a.query("SHOW DATABASES").fetchall())


result = a.query("SELECT * FROM information_schema.CHARACTER_SETS")
a.save("menagerie", "output_result", result, "append")
print(a.query("SELECT * FROM menagerie.output_result").fetchall())


# for x in result:
#     print(result.rowcount)

# a.save("SNOWFLAKE_SAMPLE_DATA", "output", result, "append")
