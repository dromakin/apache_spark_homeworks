# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Spark Streaming Homework
# MAGIC ## Description
# MAGIC * Organize incremental copy of hotel/weather data from Azure ADLS gen2 storage into provisioned with terraform Azure ADLS gen2 storage (with a delay, one day per cycle).
# MAGIC * Create Databricks Notebooks (Azure libraries like hadoop-azure and azure-storage are already part of Databricks environment, details are described here). Use ABFS drivers and OAuth credentials like below:
# MAGIC * Create Spark Structured Streaming application with Auto Loader to incrementally and efficiently processes hotel/weather data as it arrives in provisioned Azure ADLS gen2 storage. Using Spark calculate in Databricks Notebooks for each city each day:
# MAGIC     * Number of distinct hotels in the city.
# MAGIC     * Average/max/min temperature in the city.
# MAGIC * Visualize incoming data in Databricks Notebook for 10 biggest cities (the biggest number of hotels in the city, one chart for one city):
# MAGIC     * X-axis: date (date of observation).
# MAGIC     * Y-axis: number of distinct hotels, average/max/min temperature.
# MAGIC * Deploy Databricks Notebook on cluster, to setup infrastructure use terraform scripts from module. Default resource parameters (specifically memory) will not work because of free tier limitations. You needed to setup memory and cores properly.
# MAGIC 
# MAGIC 
# MAGIC ### Expected results
# MAGIC * Repository with notebook (with output results), configuration scripts, application sources, execution plan dumps, analysis and etc.
# MAGIC * Upload in task Readme MD file with link on repo, fully documented homework with screenshots and comments.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pyspark.sql.types import StringType,BooleanType,DateType
from pyspark import Row

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define application data paths

# COMMAND ----------

# set config path
HOTEL_WEATHER_PATH = "abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather"
ROOT_PATH = '/m13sparkstreaming_python_azure'

ACC_NAME = "stdromakinwesteurope"
SA_CONTAINER = "data"
APP_PATH = f"abfss://{SA_CONTAINER}@{ACC_NAME}.dfs.core.windows.net/hotel-weather/"

# bronze
BRONZE_PATH = f'{ROOT_PATH}/bronze'
BRONZE_DATA = f'{BRONZE_PATH}/data'
BRONZE_CHECKPOINT = f'{BRONZE_PATH}/checkpoint'

# silver
SILVER_PATH = f'{ROOT_PATH}/silver'
SILVER_DATA = f'{SILVER_PATH}/data'
SILVER_CHECKPOINT = f'{SILVER_PATH}/checkpoint'

# gold
GOLD_PATH = f'{ROOT_PATH}/gold'
GOLD_DATA = f'{GOLD_PATH}/data'
GOLD_CHECKPOINT = f'{GOLD_PATH}/checkpoint'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure Spark

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net", "f3905ff9-16d4-43ac-9011-842b661d556d")
spark.conf.set("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net", "mAwIU~M4~xMYHi4YX_uT8qQ.ta2.LTYZxT")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net", "https://login.microsoftonline.com/b41b72d0-4e9f-4c26-8a69-f949f367c91d/oauth2/token")

spark.conf.set(f"fs.azure.account.key.{ACC_NAME}.dfs.core.windows.net", "zz0m/lUWe3Uiog5Ufv8ZVLBD6QVLVk7mv7ZgJLjR3ztNJAyxjGUKsbPZT4wvUYTl2NAtmqN8qhJZYDCcz8YMig==")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Configure Database

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS m13sparkstreaming;
# MAGIC USE m13sparkstreaming;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main steps

# COMMAND ----------

display(dbutils.fs.ls(HOTEL_WEATHER_PATH))

# COMMAND ----------

# clean paths
dbutils.fs.rm(SILVER_PATH, recurse=True)
dbutils.fs.rm(SILVER_CHECKPOINT, recurse=True)
 
dbutils.fs.rm(GOLD_PATH, recurse=True)
dbutils.fs.rm(GOLD_CHECKPOINT, recurse=True)

dbutils.fs.rm(BRONZE_PATH, True)
# dbutils.fs.rm(SILVER_PATH, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define schema for source data

# COMMAND ----------

hotel_weather_schema="""
  address STRING,
  avg_tmpr_c DOUBLE,
  avg_tmpr_f DOUBLE,
  city STRING,
  country STRING,
  geoHash STRING,
  id STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  name STRING,
  wthr_date STRING,
  year STRING,
  month STRING,
  day STRING
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading input data and running bronze stream

# COMMAND ----------

hotel_weather_df = (
  spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "parquet")
  .option("cloudFiles.maxFilesPerTrigger", 100)
  .option("cloudFiles.maxBytesPerTrigger", "10k")
  .option("cloudFiles.partitionColumns", "year, month, day")
  .schema(hotel_weather_schema)
  .load(HOTEL_WEATHER_PATH)
)

display(hotel_weather_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save stream of hotel_weather_df as bronze delta files

# COMMAND ----------

(
    hotel_weather_df.writeStream
    .format("delta")
    .option("checkpointLocation", BRONZE_CHECKPOINT)
    .queryName("bronze_stream")
    .outputMode("append")
    .start(BRONZE_DATA)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read bronze data

# COMMAND ----------

# hotel_weather_silver_df = spark.readStream.format("delta").load(BRONZE_DATA).dropna()

bronze_df = spark.readStream.format('delta').load(BRONZE_DATA)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform bronze data

# COMMAND ----------

transformed_bronze_df = (
    bronze_df.select(
        'id', 
        F.col('address').alias('name'), 
        F.col('name').alias('address'),
        'avg_tmpr_c',
        'avg_tmpr_f',
        'city',
        'country',
        'geoHash',
        'latitude',
        'longitude',
        'wthr_date',
        'year',
        'month',
        'day'
    )
    .dropna()
)

# COMMAND ----------

display(transformed_bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Running Silver stream

# COMMAND ----------

(
    transformed_bronze_df.writeStream
    .format("delta")
    .option("checkpointLocation", SILVER_CHECKPOINT)
    .queryName("silver_stream")
    .outputMode("append")
    .start(SILVER_DATA)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read silver data

# COMMAND ----------

silver_df = (
    spark.readStream
      .format('delta')
      .load(SILVER_DATA)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Running Gold stream

# COMMAND ----------

gold_df = (
    silver_df
    .groupBy('city', 'wthr_date')
    .agg(
        F.approx_count_distinct('id').alias('distinct_hotels'),
        F.round(F.avg('avg_tmpr_c'), 1).alias('avg_t'),
        F.round(F.max('avg_tmpr_c'), 1).alias('max_t'),
        F.round(F.min('avg_tmpr_c'), 1).alias('min_t')
    )
    .select('city', 'wthr_date', 'distinct_hotels', 'avg_t', 'max_t', 'min_t')
)

# COMMAND ----------

display(gold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save gold data as delta files

# COMMAND ----------

(
    gold_df.writeStream
    .format("delta")
    .option("checkpointLocation", GOLD_CHECKPOINT)
    .queryName("gold_stream")
    .outputMode("complete")
    .start(GOLD_DATA)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register Gold table in the Metastore

# COMMAND ----------

spark.sql(
"""
    DROP TABLE IF EXISTS hotel_weather_gold
"""
)
 
spark.sql(
f"""
    CREATE TABLE hotel_weather_gold
    USING DELTA
    LOCATION "{GOLD_DATA}"
"""
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Spark Structured Streaming application with Auto Loader to incrementally and efficiently processes hotel/weather data as it arrives in provisioned Azure ADLS gen2 storage
# MAGIC 
# MAGIC ### Using Spark calculate in Databricks Notebooks for each city each day:
# MAGIC * Number of distinct hotels in the city.
# MAGIC * Average/max/min temperature in the city.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hotel_weather_gold LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create gold table for top 10 cities by number of hotels

# COMMAND ----------

spark.sql(f"""
DROP TABLE IF EXISTS top_10_cities
""")
 
spark.sql(f"""
CREATE TABLE top_10_cities 
USING DELTA LOCATION "{GOLD_DATA}/top-10-cities" AS  
WITH cte AS 
( 
  SELECT 
    city,
    max(distinct_hotels) AS num_of_hotels,
    row_number() OVER (ORDER BY max(distinct_hotels) DESC) AS rank 
  FROM hotel_weather_gold
  GROUP BY city
  LIMIT 10
) SELECT * FROM cte
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 10 cities by number of hotels

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM top_10_cities

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize incoming data for 10 biggest cities (the biggest number of hotels in the city, one chart for one city):
# MAGIC * X-axis: date (date of observation).
# MAGIC * Y-axis: number of distinct hotels, average/max/min temperature.

# COMMAND ----------

hotel_weather_df = spark.read.format("delta").load(GOLD_DATA)
hotel_by_city = (
    hotel_weather_df
    .groupBy("city")
    .agg(F.max("distinct_hotels").alias("distinct_hotels_num"))
    .withColumn("rank", F.row_number().over(Window.orderBy(F.col("distinct_hotels_num").desc())))
    .limit(10)
)

display(hotel_by_city)

# COMMAND ----------

top_10_biggest_cities_by_hotel = hotel_weather_df.join(hotel_by_city, on="city")

display(top_10_biggest_cities_by_hotel)

# COMMAND ----------

for i in range(11):
    display(top_10_biggest_cities_by_hotel.filter(top_10_biggest_cities_by_hotel.rank == i).orderBy("wthr_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize incoming data for 10 biggest cities SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   hwg.wthr_date,
# MAGIC   hwg.distinct_hotels,
# MAGIC   hwg.avg_t,
# MAGIC   hwg.max_t,
# MAGIC   hwg.min_t
# MAGIC FROM hotel_weather_gold AS hwg
# MAGIC JOIN top_10_cities AS tc ON hwg.city = tc.city
# MAGIC WHERE tc.rank = 1
# MAGIC ORDER BY hwg.wthr_date

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execution plan

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN EXTENDED
# MAGIC SELECT
# MAGIC   hwg.wthr_date,
# MAGIC   hwg.distinct_hotels,
# MAGIC   hwg.avg_t,
# MAGIC   hwg.max_t,
# MAGIC   hwg.min_t
# MAGIC FROM hotel_weather_gold AS hwg
# MAGIC JOIN top_10_cities AS tc ON hwg.city = tc.city
# MAGIC WHERE tc.rank = 3
# MAGIC ORDER BY hwg.wthr_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stop all streams

# COMMAND ----------

for streams in spark.streams.active:
    streams.stop()
