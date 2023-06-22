# Databricks notebook source
import dlt
import pyspark.sql.functions as F
from pyspark.sql import Row
from datetime import date

source = spark.conf.get("source")
today = date.today()
valid_dates = { "date_is_not_null": F.col("Date_Created_Date").isNotNull(), "date_is_not_future":F.col("Date_Created_Date") < today }

@dlt.table(
    comment = "All records must be there in bronze table",
    table_properties = {"quality": "bronze"})
def menudata_bronze():
    return (spark.read.format("csv")
            .load(f"{source}/menu_data.csv", header = "true", inferSchema = True)
            .select(F.current_timestamp().alias("processing_time"), 
                F.input_file_name().alias("source_file"),
                F.to_date(F.col("Date_Created"),"MM/dd/yyyy").alias("Date_Created_Date"),
                "*"))

@dlt.table(
    comment = "Append only items with valid timestamps.",
    table_properties = {"quality": "silver"})
@dlt.expect_all_or_drop(valid_dates)
def menudata_silver():
    return (
        spark.readStream
            .option("skipChangeCommits", "true")
            .option("ignoreChanges", "true")
            .table("LIVE.menudata_bronze")
    )

@dlt.table(
    comment = "Group by calories",
    table_properties = {"quality": "gold"})
def menu_by_calories():
    return (
        dlt.read("menudata_silver")
            .orderBy(["Calories"])
            .agg(F.count(F.when(F.col("LIVE.menudata_silver.Calories") <= 250, True)).alias("Under_250"),
                 F.count(F.when((F.col("LIVE.menudata_silver.Calories") > 250) & (F.col("LIVE.menudata_silver.Calories") <= 500), True)).alias("Under_500"),
                 F.count(F.when((F.col("LIVE.menudata_silver.Calories") > 500) & (F.col("LIVE.menudata_silver.Calories") <= 750), True)).alias("Under_750"),
                 F.count(F.when((F.col("LIVE.menudata_silver.Calories") > 750) & (F.col("LIVE.menudata_silver.Calories") <= 1000), True)).alias("Under_1000"),
                 F.count(F.when(F.col("LIVE.menudata_silver.Calories") > 1000, True)).alias("Over_1000"))
    )

@dlt.table(
    comment = "Group by category",
    table_properties = {"quality": "gold"})
def menu_by_category():
    return (
        dlt.read("menudata_silver")
            .groupBy(F.col("Category"))
            .agg(F.count("*").alias("total_categories"))
    )


# COMMAND ----------

# bronze = "sudhir_bezawada_tctu_da_delp_pipeline_demo.menudata_bronze"
# silver = "sudhir_bezawada_tctu_da_delp_pipeline_demo.menudata_silver"
# junk   = "Junk_Table"
# where = "Item like \"%-Date%\""

# def print_table_contents(table):
#     print("===============================BEGIN==================================")
#     if(spark.catalog.tableExists(table)):
#         print("Table exists:" , table)
#         mytable = spark.table(table)
#         mywhere = mytable.where(where)
#         print("Total Row Count: ", mytable.count(), ", Invalid Row Count: ", mywhere.count())
#         mywhere.select("Category", "Item").show()
#     else:
#         print("Table does not exist:" , table)
#     print("================================END===================================")

# print_table_contents(bronze)
# print_table_contents(silver)
# print_table_contents(junk)

# import pyspark.sql.functions as F
# from pyspark.sql import Row

# df = spark.table("sudhir_bezawada_tctu_da_delp_pipeline_demo.menudata_silver").orderBy(["Calories"])
# newdf = spark.createDataFrame([Row("Zero_to_250", df.filter("Calories <= 250").count()), 
#                                Row("251_to_500", df.filter("Calories > 250 and Calories <= 500").count()),
#                                Row("501_to_750", df.filter("Calories > 500 and Calories <= 750").count()),
#                                Row("751_to_1000", df.filter("Calories > 750 and Calories <= 1000").count()),
#                                Row("1000+", df.filter("Calories > 1000").count())], 
#                               schema='Range string, Range_Count int')
# newdf.show()
