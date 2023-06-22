# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

import dlt
import pandas as pd
from pyspark.sql.functions import (regexp_replace, col)
from pyspark.sql.types import (StructField, StringType, StructType, IntegerType)

filesource = spark.conf.get("filesource")
credits_schema = StructType(fields = [StructField('person_id', IntegerType(), True), 
                  StructField('id', StringType(), True),
                  StructField('name', StringType(), True),
                  StructField('character', StringType(), True),
                  StructField('role', StringType(), True)])
@dlt.table ( 
    name = "subscribers_bronze",
    comment = "All records must be there in bronze table",
    table_properties = {"quality":"bronze"})
def subscribers_bronze(): 
    pdf = pd.read_excel(f"{filesource}/Subscribers.xlsx")
    sdf = spark.createDataFrame(pdf)
    return (sdf)

@dlt.table(
    comment="All records must be there in bronze table", 
    table_properties={"quality": "bronze"})
def titles_bronze(): 
    pdf = pd.read_excel(f"{filesource}/titles.xlsx")
    sdf = spark.createDataFrame(pdf)
    return (sdf)

@dlt.table(
    comment="All records must be there in bronze table", 
    table_properties={"quality": "bronze"})
def credits_bronze(): 
    pdf = pd.read_excel(f"{filesource}/credits.xlsx", dtype=str)
    sdf = spark.createDataFrame(pdf)
    sdf = sdf.withColumn("person_id", regexp_replace(col("person_id"), "\'", "").cast("int"))
    return (sdf)
