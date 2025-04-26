from snowflake.snowpark import Session

def snowpark_basic_auth() -> Session:
    """
    Create a Snowpark session using basic authentication.
    """
    # Snowflake connection parameters
    conn_params = {
        "account": "<account>",
        "user": "1242sanjay",
        "password": "<password>"
    }

    # Create a Snowpark session
    session = Session.builder.configs(conn_params).create()
    
    return session

if __name__ == "__main__":
    session = snowpark_basic_auth()
    print("Snowpark session created successfully.")
    # Perform operations with the session
    session.sql("SELECT CURRENT_SESSION()").collect()
    # Close the session when done
    session.close()