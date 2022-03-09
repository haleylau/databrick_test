# Databricks notebook source
# MAGIC %md
# MAGIC # Censis Analysis test

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/census_clean.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)


# COMMAND ----------

df = df.drop("_c0")

# COMMAND ----------

df.dtypes

# COMMAND ----------

display(df)

# COMMAND ----------

display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "census_clean_csv"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

df.describe()

# COMMAND ----------


