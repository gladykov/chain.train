from db.connection_snowflake import SnowflakeConnection
from db.connection_mariadb import MariaDBConnection

a = MariaDBConnection("name")


# r = a.query("show tables")
#
# print(a.parse_result(r))
# print("---")
# d = a.query("select * from CALL_CENTER limit 10")
# print(a.parse_result(d))

result = a.query("SHOW TABLES")
for x in result:
    print(result)

# a.save("SNOWFLAKE_SAMPLE_DATA", "output", result, "append")
