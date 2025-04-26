from snowpark02_basic_auth import snowpark_basic_auth
from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType, TimestampType


session = snowpark_basic_auth()

session.sql("use demo.snowpark").collect()

# put command to put the file in the user stage
# session.file.put("./data/location.csv", "@~", auto_compress=False)

# define the schema for the csv file
location_schema = StructType([
    StructField("location_id", IntegerType(), nullable=False),
    StructField("city", StringType(), nullable=True),
    StructField("state_province", StringType(), nullable=True),
    StructField("postal_code", StringType(), nullable=True),
    StructField("active_flag", StringType(), nullable=True),
    StructField("created_at", TimestampType(), nullable=True),
    StructField("updated_at", TimestampType(), nullable=True)
])

# load csv file from user stage
df1 = session.read.options({"skip_header":1, "field_delimiter":","}).csv("@~/location.csv")
df2 = session.read.options({"skip_header":0, "field_delimiter":","}).csv("@~/location.csv")
df3 = session.read.schema(location_schema).options({"skip_header":1, "field_delimiter":","}).csv("@~/location.csv")

df1.show()
df2.show()
df3.show()
# df3.explain()


df3.write.save_as_table(
    table_name=["demo", "snowpark", "location"],  # Replace with your desired table name
    mode="errorifexists",             # Options: "overwrite", "append", "errorifexists", "ignore"
    table_type="transient"        # Options: "", "temp", "temporary", "transient"
)


session.close()


