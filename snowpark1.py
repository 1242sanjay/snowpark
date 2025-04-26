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


def main(session): 
    warehouse_name = 'MY_XSMALL_WH'
    session.use_warehouse(warehouse_name)

    # Use SQL to create our Tasty Bytes Database
    session.sql('CREATE OR REPLACE DATABASE sample_db;').collect()

    # Use SQL to create our Raw POS (Point-of-Sale) Schema
    session.sql('CREATE OR REPLACE SCHEMA sample_db.my_schema;').collect()

    session.sql('CREATE OR REPLACE TABLE sample_db.my_schema.sample_product_data (id INT, parent_id INT, category_id INT, name VARCHAR, serial_number VARCHAR, key INT, "3rd" INT)').collect()
    session.sql("""INSERT INTO sample_product_data VALUES (1, 0, 5, 'Product 1', 'prod-1', 1, 10),
    (2, 1, 5, 'Product 1A', 'prod-1-A', 1, 20),
    (3, 1, 5, 'Product 1B', 'prod-1-B', 1, 30),
    (4, 0, 10, 'Product 2', 'prod-2', 2, 40),
    (5, 4, 10, 'Product 2A', 'prod-2-A', 2, 50),
    (6, 4, 10, 'Product 2B', 'prod-2-B', 2, 60),
    (7, 0, 20, 'Product 3', 'prod-3', 3, 70),
    (8, 7, 20, 'Product 3A', 'prod-3-A', 3, 80),
    (9, 7, 20, 'Product 3B', 'prod-3-B', 3, 90),
    (10, 0, 50, 'Product 4', 'prod-4', 4, 100),
    (11, 10, 50, 'Product 4A', 'prod-4-A', 4, 100),
    (12, 10, 50, 'Product 4B', 'prod-4-B', 4, 100)""").collect()
    
    # session.sql("SELECT count(*) FROM sample_db.my_schema.sample_product_data").collect()

    product_df = session.table('sample_db.my_schema.sample_product_data')

    return product_df

session = create_session_object()
product_df = main(session)

product_df.show()

# Create a DataFrame with one column named a from specified values.
df1 = session.create_dataframe([1, 2, 3, 4]).to_df("a")
df1.show()

# Create a DataFrame with 4 columns, "a", "b", "c" and "d".
df2 = session.create_dataframe([[1, 2, 3, 4]], schema=["a", "b", "c", "d"])
df2.show()

# Create another DataFrame with 4 columns, "a", "b", "c" and "d".
from snowflake.snowpark import Row
df3 = session.create_dataframe([Row(a=1, b=2, c=3, d=4)])
df3.show()

# Create a DataFrame and specify a schema
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
schema = StructType([StructField("a", IntegerType()), StructField("b", StringType())])
df4 = session.create_dataframe([[1, "snow"], [3, "flake"]], schema)
df4.show()


