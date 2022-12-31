from db.connection_snowflake import SnowflakeConnection

a = SnowflakeConnection("name")


r = a.query("show tables")

print(a.parse_result(r))
print("---")
d = a.query("select * from CALL_CENTER limit 10")
print(a.parse_result(d))

a.close()