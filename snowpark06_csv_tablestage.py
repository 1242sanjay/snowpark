# use snowsql cli and put the file in table stage
# put file://data/location.csv @%location auto_compress=false overwrite=true;

from snowpark02_basic_auth import snowpark_basic_auth
from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType, TimestampType
from snowflake.snowpark.functions import col, in_

session = snowpark_basic_auth()
session.sql("use demo.snowpark").collect()

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

session.sql("ALTER SESSION SET TIMESTAMP_INPUT_FORMAT = 'DD-MM-YYYY HH24:MI'").collect()

# load csv file from table stage
df3 = session.read.schema(location_schema).options({"skip_header":1, "field_delimiter":","}).csv("@%location/location.csv")

df3.show()

# use order by to sort the data
ordered_df = df3.order_by(col("city").asc(), col("state_province").asc())
ordered_df.show()

active_loc_df = df3.filter(col("active_flag") == "Yes").order_by(col("city").asc(), col("state_province").asc())
active_loc_df.show()

# in operations, we can use the filter function to filter the data
postal_code_df = df3.filter((col("postal_code").in_(["282001", "110001", "400001"]))).order_by(col("city").asc(), col("state_province").asc())

postal_code_df.show()
session.close()