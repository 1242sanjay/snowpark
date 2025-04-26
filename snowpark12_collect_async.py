from snowpark02_basic_auth import snowpark_basic_auth
from snowflake.snowpark.functions import col, lit
import sys
import logging

# Set up logging configuration
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

session = snowpark_basic_auth()

cust_df = session.sql("select * from demo.snowpark.customer_data_01")

my_param_collect = {"query_tag": "test-01"}
# cust_df.collect(statement_params=my_param_collect)
# async_obj = cust_df.collect_async(statement_params=my_param_collect)
async_obj = cust_df.collect_nowait(statement_params=my_param_collect)

# lets check the data type of async_obj
logging.info("<----------------- async_obj ----------------->"+ str(async_obj))
logging.info("<----------------- async_obj type ----------------->"+ str(type(async_obj)))
logging.info("<----------------- async_obj query_id ----------------->"+ str(async_obj.query_id))
logging.info("<----------------- async_obj.is_done ----------------->" + str(async_obj.is_done()))
logging.info("<----------------- async_obj.result ----------------->" + str(async_obj.result()))

cust_df.show()
session.close()
