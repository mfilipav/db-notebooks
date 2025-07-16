# Databricks notebook source
# MAGIC %md
# MAGIC ### View the latest COVID-19 hospitalization data
# MAGIC #### Setup 

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get and Transform data

# COMMAND ----------

# notebook params, passed with `notebook_task.base_parameters`
default_data_path = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/hospitalizations/covid-hospitalizations.csv'
default_table_version = "v0"  # Update this when logic changes significantly

# In your Databricks notebook
dbutils.widgets.text("data_path", default_data_path, "Path to COVID hospitalizations data")

dbutils.widgets.text("table_version", default_table_version, "Table version")

# Access the parameters
data_path = dbutils.widgets.get("data_path")
table_version = dbutils.widgets.get("table_version")

print(f"Data path: {data_path}")
print(f"Table version: {table_version}")

# COMMAND ----------

from covid_analysis.transforms import (
    filter_country, pivot_and_clean, clean_spark_cols, index_to_col,
)
import pandas as pd

df = pd.read_csv(data_path)
df = filter_country(df, country='DZA')
df = pivot_and_clean(df, fillna=0)

# The current schema has spaces in the column names, which are incompatible with Delta Lake. 
# To save our data as a table, we'll replace the spaces with underscores.
df = clean_spark_cols(df)

# We also need to add the date index as its own column,
# or it won't be available to others who might query this table.
df = index_to_col(df, colname='date')

# Convert from Pandas to a pyspark sql DataFrame.
df = spark.createDataFrame(df)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save to Delta Lake (and update the table version if needed!)

# COMMAND ----------

# Write to Delta Lake
# Define at the top of notebook
table_name = f"covid_stats_{table_version}"

df.write.mode('overwrite').option("comment", "Updated data processing logic, new filtering criteria").saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Visualize

# COMMAND ----------

# Using Databricks visualizations and data profiling
# display(spark.table(table_name))

# COMMAND ----------

# Using python
# df.toPandas().plot(figsize=(13,6), grid=True).legend(loc='upper left');

# COMMAND ----------


