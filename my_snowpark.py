# Import required libraries
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col

# establishing a connection
def create_session_object():
    connection_parameters = {
        "account":"<account>",
        "user":"1242sanjay",
        "password":"<password>",
        "role":"SYSADMIN",
        "warehouse":"COMPUTE_WH",
        "database":"SNOWFLAKE_SAMPLE_DATA",
        "schema":"TPCH_SF1"
    }

    # Create the session object with the provided connection parameters
    session = Session.builder.configs(connection_parameters).create()
    print("Connected")
    return session

# # function call
# create_session_object()

def create_dataframe(session):

    # selecting warehouse
    warehouse_name = 'MY_XSMALL_WH'
    session.use_warehouse(warehouse_name)
  
    # Create a dataframe
    df_table = session.table("CUSTOMER")

    #---------------------------------
    # **ACTIONS**
    #---------------------------------

    # count method
    df_table.count()
    print(df_table.count()) 

    # show method
    # df_table.show()

    # collect method
    # df_results = df_table.collect()
    # print(df_results) 

    #---------------------------------
    # **TRANSFORMATIONS **
    #---------------------------------
  
    df_filtered = df_table.filter((col("C_ACCTBAL") < 200) & (col("C_ACCTBAL") > 100))
    print(df_filtered.count())
  
    # Chaining method calls
    df_filtered = df_table.filter((col("C_ACCTBAL") < 200) & (col("C_ACCTBAL") > 100)).sort(col("C_ACCTBAL").desc()).limit(10)
    print(df_filtered.count())
    df_filtered.show()
  
    # df_filtered.collect()
  
    df_filtered_persisted = df_filtered.collect()
    print(df_filtered_persisted)



session = create_session_object()
create_dataframe(session)

