# Databricks notebook source
# MAGIC %md
# MAGIC # Census Plotly test test

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform a basic exploratory data analysis on the census data

# COMMAND ----------

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get data

# COMMAND ----------

def get_census_data():
    # column names
    cols = ['age', 'workclass', 'fnlwgt', 'education', 
            'education_num', 'marital_status', 'occupation',
            'relationship', 'race', 'sex', 'capital_gain',
            'capital_loss', 'hours_per_week', 'native_country', 
            'over_fifty_k']
    
    url = 'https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data'
    return pd.read_csv(url, names=cols)

# COMMAND ----------

df = get_census_data()

# COMMAND ----------

df["over_fifty_k_bool"] = df.over_fifty_k.str.strip() == ">50K"

# COMMAND ----------

df.dtypes

# COMMAND ----------

fig = go.Figure()
fig.add_trace(go.Histogram(x=df.age,nbinsx=50))

# The two histograms are drawn on top of another
fig.update_layout(barmode='stack',)
fig.show()

# COMMAND ----------

fig = px.histogram(df, x=df.fnlwgt.agg('log'), color="over_fifty_k_bool", log_x=False,log_y=False,
                         hover_data=df.columns)
fig.show()

# COMMAND ----------


fig = go.Figure(data=[
    go.Bar(name='True', x=df.occupation.unique(), y=df.loc[df.over_fifty_k_bool == True, "occupation"].value_counts()),
    go.Bar(name='False', x=df.occupation.unique(), y=df.loc[df.over_fifty_k_bool == False, "occupation"].value_counts())
])
# Change the bar mode
fig.update_layout(barmode='stack')
fig.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Pandas to Spark

# COMMAND ----------

df

# COMMAND ----------

from pyspark.sql import SparkSession
#Create PySpark SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()
#Create PySpark DataFrame from Pandas
sparkDF=spark.createDataFrame(df) 
sparkDF.printSchema()
sparkDF.show()

# COMMAND ----------

# Create a view or table

temp_table_name = "census_clean_from_raw"

sparkDF.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select * from `census_clean_from_raw`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select * from `census_clean_from_raw`

# COMMAND ----------


