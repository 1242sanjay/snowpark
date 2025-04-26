from snowpark02_basic_auth import snowpark_basic_auth

session = snowpark_basic_auth()

df = session.sql("SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER")

my_param_show = {"query_tag": "testing-show"}
my_param_collect = {"query_tag": "testing-collect"}

df.show(10, max_width=50, statement_params=my_param_show)
customer_list = df.collect(statement_params=my_param_collect, case_sensitive=False)
for element in customer_list:
    print("type:", type(element))
    print("element:", element)
    print("element as dict:", element.asDict())
    break

session.close()