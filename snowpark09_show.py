from snowpark02_basic_auth import snowpark_basic_auth

session = snowpark_basic_auth()

df = session.sql("select * from demo.snowpark.customer_data_03")

stat_params = {"query_tag": "snowpark-df-show"}

df.show(10, max_width=10, statement_params=stat_params)

session.close()