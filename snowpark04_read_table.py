# from snowflake.snowpark import Session
from snowpark02_basic_auth import snowpark_basic_auth

session = snowpark_basic_auth()
print(session)

session.sql("use demo.snowpark").collect()

print("Current role name", session.get_current_role())
print("Current warehouse name", session.get_current_warehouse())
print("Current database name", session.get_current_database())
print("Current schema name", session.get_current_schema())
print("Fully qualified schema name", session.get_fully_qualified_current_schema())

# call the table function on session object
df = session.table("employee")
df.show()

# alternate approch
database = "demo"
schema = "snowpark"
emp_df = session.table([database, schema, "employee"])
# emp_df = session.table(f"{database}.{schema}.employee")
emp_df.show()

# close the session
session.close()

