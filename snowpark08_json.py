from snowpark02_basic_auth import snowpark_basic_auth

session = snowpark_basic_auth()
session.sql("use role sysadmin").collect()
session.sql("use demo.snowpark").collect()

df = session.read.json("@snowpark/demo/customer_data01.json")
df.show(2)
df_schema = df.schema
print("Schema object:", df_schema)
print("Type of schema object:", type(df_schema))
print("Number of columns in schema:", len(df_schema.fields))

# iterate through the schema fields
for col in df_schema:
    print(f"{col.name}: {col.datatype}")

# save the data as table
df.write.save_as_table("customer_data_02", mode="overwrite", table_type="transient")

new_df = session.sql("select $1:CustomerID::number as CustomerID,\
                             $1:Name::text as Name,\
                             $1:Email::text as Email,\
                             $1:Age::text as Age,\
                             $1:City::text as City\
                        from customer_data_02")
new_df.show()

# save the data as table
new_df.write.save_as_table("customer_data_03", mode="overwrite")

session.close()

