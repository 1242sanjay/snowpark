from snowpark02_basic_auth import snowpark_basic_auth
from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType, TimestampType

session = snowpark_basic_auth()
session.sql("use demo.snowpark").collect()

# read parquet file from internal stage
df = session.read.parquet("@snowpark/demo/customer_data.parquet")
df.show()
df_schema = df.schema
print("Schema object:", df_schema)
print("Type of schema object:", type(df_schema))
print("Number of columns in schema:", len(df_schema.fields))

# save the data as table
df.write.save_as_table("customer_data_01", mode="overwrite")

session.close()