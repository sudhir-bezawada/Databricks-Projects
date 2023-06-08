# Databricks notebook source
# MAGIC %run ./include-notebook

# COMMAND ----------

print ("Databricks Notebook - Python Cell")
display(dbutils.fs.ls("/databricks-datasets"))

covid_files = dbutils.fs.ls("/databricks-datasets/COVID")
display(covid_files)

airlines_files = dbutils.fs.ls("/databricks-datasets/airlines")
display(airlines_files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Databricks Notebook Python Cell!";

# COMMAND ----------

# MAGIC %md # This is a header
# MAGIC This is a mark down cell
# MAGIC
# MAGIC <b>This is bold text</b>
# MAGIC
# MAGIC <h1>Heading 1</h1>
# MAGIC
# MAGIC <h2>Heading 2</h2>
# MAGIC
# MAGIC <h3>Heading 3</h3>
# MAGIC
# MAGIC <h4>Heading 4</h4>
# MAGIC
# MAGIC <h5>Heading 5</h5>
# MAGIC
# MAGIC # Title One
# MAGIC ## Title Two
# MAGIC ### Title Three
# MAGIC
# MAGIC This is in <b><i>bold italics</i></b>
# MAGIC
# MAGIC This is a link to access <a href="https://www.google.com">Google</a>
# MAGIC
# MAGIC <b>Bullet points</b>
# MAGIC - Bullet 1
# MAGIC - Bullet 2
# MAGIC - Bullet 3
# MAGIC Links/Embedded HTML: <a href="https://en.wikipedia.org/wiki/Markdown" target="_blank">Markdown - Wikipedia</a>
# MAGIC
# MAGIC Images:
# MAGIC ![Spark Engines](https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png)
