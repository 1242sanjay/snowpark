from snowpark02_basic_auth import snowpark_basic_auth
from snowflake.snowpark.functions import col, lit
import sys
import logging

# Set up logging configuration
logging.basicConfig(
    # stream=sys.stdout,
    filename="snowpark11_logging.log",
    # level=logging.INFO,
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
print("#################### Logging Operations ####################")

session = snowpark_basic_auth()
print("#################### Session Operations ####################")

cust_df = session.sql("select * from demo.snowpark.customer_data_01")
# getting all customers with age less than 30
cust_below_30 = cust_df.filter(col('"Age"') < 30)
# adding a column as unmarried
cust_below_30 = cust_below_30.withColumn("Marital Status",lit("Single"))

print("#################### Dataframe Operations ####################")
cust_df.show()
cust_below_30.show()
print("#################### Show Operations ####################")
session.close()
print("#################### Session Close ####################")