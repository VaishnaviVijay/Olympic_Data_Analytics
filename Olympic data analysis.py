# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "a76c9da7-06f2-4960-a0cb-9dde4254e2bb",
"fs.azure.account.oauth2.client.secret": 'iUI8Q~EN.DCFmENJ5fmnQTlzWTk3E5x0XEP7.cet',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/059c3bde-b728-4729-994c-2430893aa8cb/oauth2/token"}

dbutils.fs.mount(
source = "abfss://olympicdata@olympicdata2624.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolymictest",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "mnt/tokyoolymictest"

# COMMAND ----------

#don't need of spark session
athletes = spark.read.format("csv").option("header","true").load("/mnt/tokyoolymictest/raw-data/athletes.csv")
coaches = spark.read.format("csv").option("header","true").load("/mnt/tokyoolymictest/raw-data/coaches.csv")
gender = spark.read.format("csv").option("header","true").load("/mnt/tokyoolymictest/raw-data/gender.csv")
medals = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymictest/raw-data/medals.csv")
teams = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymictest/raw-data/teams.csv")

# COMMAND ----------

athletes.show()

# COMMAND ----------

athletes.printSchema()

# COMMAND ----------

coaches.show()

# COMMAND ----------

coaches.printSchema()

# COMMAND ----------

gender.show()

# COMMAND ----------

gender.printSchema()

# COMMAND ----------

gender = gender.withColumn("Female",col("Female").cast(IntegerType()))\
    .withColumn("Male",col("Male").cast(IntegerType()))\
    .withColumn("Total",col("Total").cast(IntegerType()))

# COMMAND ----------

gender.printSchema()

# COMMAND ----------

medals.show()

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

teams.show()

# COMMAND ----------

teams.printSchema()

# COMMAND ----------

total_gold_medals_country = medals.orderBy("Gold",ascending=False).select("Team_Country","Gold").show()

# COMMAND ----------



# COMMAND ----------

athletes.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymictest/transformed-data/athletes")

# COMMAND ----------

coaches.write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymictest/transformed-data/coaches")
gender.write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymictest/transformed-data/gender")
medals.write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymictest/transformed-data/medals")
teams.write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymictest/transformed-data/teams")

# COMMAND ----------



# COMMAND ----------


