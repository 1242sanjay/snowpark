from snowflake.snowpark import Session

connection_parameters = {
    "account": "<account>",
    "user": "1242sanjay",
    "password": "<password>",
}

# print the connection parameters
print("The connection parameters are:", connection_parameters)

# Create a session object with the provided account parameters
session = Session.builder.configs(connection_parameters).create()

# print values from session object to test
print("The session is:", session)
print("The account is:", session.get_current_account())
print("The user is:", session.get_current_user())
print("The database is:", session.get_current_database())
print("The schema is:", session.get_current_schema())
print("The warehouse is:", session.get_current_warehouse())
print("The role is:", session.get_current_role())
# print("Fully qualified schema is:", session.get_fully_qualified_current_schema()) #this will give error if schema is not set.
print("The session is:", session.get_active_session())
print("The session is :", session.getActiveSession()) # this is deprecated

# close the session
session.close()
print("The session is closed.")
