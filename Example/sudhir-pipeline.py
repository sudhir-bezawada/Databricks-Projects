# Databricks notebook source
import dlt
import pyspark.sql.functions as F
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
        # dlt.read_stream("menudata_bronze").select(
        #         "Category",
        #         "Item",
        #         "Date_Created_Date",
        #         "Serving_Size",
        #         "Calories",
        #         "Calories_from_Fat",
        #         "Total_Fat",
        #         "Total_Fat_%_Daily_Value",
        #         "Saturated_Fat",
        #         "Saturated_Fat_%_Daily_Value",
        #         "Trans_Fat",
        #         "Cholesterol",
        #         "Cholesterol_%_Daily_Value",
        #         "Sodium",
        #         "Sodium_%_Daily_Value",
        #         "Carbohydrates",
        #         "Carbohydrates_%_Daily_Value",
        #         "Dietary_Fiber",
        #         "Dietary_Fiber_%_Daily_Value",
        #         "Sugars",
        #         "Protein"
        #     )
    )

# @dlt.table
# def orders_bronze():
#     return (
#         spark.readStream
#             .format("cloudFiles")
#             .option("cloudFiles.format", "json")
#             .option("cloudFiles.inferColumnTypes", True)
#             .load(f"{source}/orders")
#             .select(
#                 F.current_timestamp().alias("processing_time"), 
#                 F.input_file_name().alias("source_file"), 
#                 "*"
#             )
#     )

# @dlt.table(
#     comment = "Append only orders with valid timestamps",
#     table_properties = {"quality": "silver"})
# @dlt.expect_or_fail("valid_date", F.col("order_timestamp") > "2021-01-01")
# def orders_silver():
#     return (
#         dlt.read_stream("orders_bronze")
#             .select(
#                 "processing_time",
#                 "customer_id",
#                 "notifications",
#                 "order_id",
#                 F.col("order_timestamp").cast("timestamp").alias("order_timestamp")
#             )
#     )

# @dlt.table
# def orders_by_date():
#     return (
#         dlt.read("orders_silver")
#             .groupBy(F.col("order_timestamp").cast("date").alias("order_date"))
#             .agg(F.count("*").alias("total_daily_orders"))
#     )

# COMMAND ----------

bronze = "sudhir_bezawada_tctu_da_delp_pipeline_demo.menudata_bronze"
silver = "sudhir_bezawada_tctu_da_delp_pipeline_demo.menudata_silver"
junk   = "Junk_Table"
where = "Item like \"%-Date%\""

def print_table_contents(table):
    print("===============================BEGIN==================================")
    if(spark.catalog.tableExists(table)):
        print("Table exists:" , table)
        mytable = spark.table(table)
        mywhere = mytable.where(where)
        print("Total Row Count: ", mytable.count(), ", Invalid Row Count: ", mywhere.count())
        mywhere.select("Category", "Item").show()
    else:
        print("Table does not exist:" , table)
    print("================================END===================================")

print_table_contents(bronze)
print_table_contents(silver)
print_table_contents(junk)
