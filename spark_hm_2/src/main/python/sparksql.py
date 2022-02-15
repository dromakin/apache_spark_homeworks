# Databricks notebook source
# MAGIC %md
# MAGIC # Spark SQL Homework

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Spark Configuration

# COMMAND ----------

# DBTITLE 1,Spark configuration
spark.conf.set("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net", "f3905ff9-16d4-43ac-9011-842b661d556d")
spark.conf.set("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net", "mAwIU~M4~xMYHi4YX_uT8qQ.ta2.LTYZxT")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net", "https://login.microsoftonline.com/b41b72d0-4e9f-4c26-8a69-f949f367c91d/oauth2/token")
# Connection to my adls storage
# "fs.azure.account.key.<storage-account-name>.dfs.core.windows.net",
spark.conf.set("fs.azure.account.key.m07.dfs.core.windows.net", "VISIlYmva1BGjlZ49cN/w7xhI9bKQKCTaf5PY7IKEbEBKPPcVsvexd9jn+v//KH11d/fsUbkOXujpK6WfZAOig==")
spark.conf.set("fs.azure.account.key.m07.blob.core.windows.net", "VISIlYmva1BGjlZ49cN/w7xhI9bKQKCTaf5PY7IKEbEBKPPcVsvexd9jn+v//KH11d/fsUbkOXujpK6WfZAOig==")

# COMMAND ----------

import os

ABFS_CONNECTION_STRING = os.getenv("ABFS_CONNECTION_STRING", "abfs://m07sparksql@bd201stacc.dfs.core.windows.net/")
# "abfss://<file-system-name>@<storage-account-name>.dfs.core.windows.net/<directory-name>"
STORAGE_LINK_STRING_ADLS_WRITE = os.getenv("STORAGE_LINK_STRING_ADLS_WRITE", "abfss://data@m07.dfs.core.windows.net")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create directory to save result and temp df

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Create container for save result

# COMMAND ----------

spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls(STORAGE_LINK_STRING_ADLS_WRITE)
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Read data from ADLS storage gen 2

# COMMAND ----------

# expedia
df_expedia = spark.read.format("avro").load(ABFS_CONNECTION_STRING + "expedia/", header=True)

# COMMAND ----------

display(df_expedia)

# COMMAND ----------

df_weather = spark.read.parquet(ABFS_CONNECTION_STRING + "/hotel-weather", header=True)

# COMMAND ----------

display(df_weather)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create a Parquet Table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Database for tables

# COMMAND ----------

username = "sparksql"

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS db_{username}")
spark.sql(f"USE db_{username}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Parquet Table for expedia

# COMMAND ----------

path_expedia = "/db/expedia/"

# COMMAND ----------

dbutils.fs.rm(path_expedia + "processed", recurse=True)

spark.sql(
    f"""
DROP TABLE IF EXISTS expedia_processed
"""
)

# COMMAND ----------

(
    df_expedia.write.mode("overwrite")
    .format("parquet")
    .save(path_expedia + "processed")
)

# COMMAND ----------

spark.sql(
    f"""
DROP TABLE IF EXISTS expedia_processed
"""
)
 
spark.sql(
    f"""
CREATE TABLE expedia_processed
USING PARQUET
LOCATION "{path_expedia}/processed"
"""
)

# COMMAND ----------

expedia_processed = spark.read.table("expedia_processed")
expedia_processed.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Parquet Table for hotel-weather

# COMMAND ----------

path_hotel_weather = "/db/hotel-weather/"

# COMMAND ----------

dbutils.fs.rm(path_hotel_weather + "processed", recurse=True)

spark.sql(
    f"""
DROP TABLE IF EXISTS hotel_processed
"""
)

# COMMAND ----------

(
    df_weather.write.mode("overwrite")
    .format("parquet")
    .save(path_hotel_weather + "processed")
)

# COMMAND ----------

spark.sql(
    f"""
DROP TABLE IF EXISTS hotel_processed
"""
)
 
spark.sql(
    f"""
CREATE TABLE hotel_processed
USING PARQUET
LOCATION "{path_hotel_weather}/processed"
"""
)

# COMMAND ----------

hotel_processed = spark.read.table("hotel_processed")
hotel_processed.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Delta tables

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta table for expedia

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED expedia_processed

# COMMAND ----------

parquet_table = f"parquet.`{path_expedia}processed`"

DeltaTable.convertToDelta(spark, parquet_table)

# COMMAND ----------

spark.sql(f"""
DROP TABLE IF EXISTS expedia_processed
""")

spark.sql(f"""
CREATE TABLE expedia_processed
USING DELTA
LOCATION "{path_expedia}/processed" 
""")

# COMMAND ----------

expedia_processed = spark.read.table("expedia_processed")
expedia_processed.count()

# COMMAND ----------

display(expedia_processed)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Delta table for hotel-weather

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE EXTENDED hotel_processed

# COMMAND ----------

parquet_table = f"parquet.`{path_hotel_weather}processed`"

DeltaTable.convertToDelta(spark, parquet_table)

# COMMAND ----------

spark.sql(f"""
DROP TABLE IF EXISTS hotel_processed
""")

spark.sql(f"""
CREATE TABLE hotel_processed
USING DELTA
LOCATION "{path_hotel_weather}/processed" 
""")

# COMMAND ----------

hotel_processed = spark.read.table("hotel_processed")
hotel_processed.count()

# COMMAND ----------

display(hotel_processed)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Analyse data using sql
# MAGIC 
# MAGIC * Using Spark SQL calculate and visualize in Databricks Notebooks (for queries use hotel_id - join key, srch_ci- checkin, srch_co - checkout:
# MAGIC   1. Top 10 hotels with max absolute temperature difference by month.
# MAGIC   2. Top 10 busy (e.g., with the biggest visits count) hotels for each month. If visit dates refer to several months, it should be counted for all affected months.
# MAGIC   3. For visits with extended stay (more than 7 days) calculate weather trend (the day temperature difference between last and first day of stay) and average temperature during stay.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS db_sparksql;
# MAGIC USE db_sparksql

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overview data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from expedia_processed 

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW COLUMNS IN expedia_processed

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hotel_processed

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Top 10 hotels with max absolute temperature difference by month.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Analyse

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT address, month, avg_tmpr_c FROM hotel_processed ORDER BY avg_tmpr_c DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT year, month, count(month) as cm FROM hotel_processed group by year, month;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT month FROM hotel_processed ORDER BY month DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC select address, month, avg_tmpr_c from hotel_processed

# COMMAND ----------

# MAGIC %sql
# MAGIC select DISTINCT address, month, MAX(avg_tmpr_c) as avg_tmpr_cmax from hotel_processed where month=8 group by address, month ORDER BY avg_tmpr_cmax DESC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select DISTINCT address, month, MAX(avg_tmpr_c) as avg_tmpr_cmax from hotel_processed group by address, month;
# MAGIC -- select DISTINCT address, month, MAX(avg_tmpr_c) as avg_tmpr_cmax from hotel_processed group by address, month ORDER BY avg_tmpr_cmax DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Idea how to do it max-min

# COMMAND ----------

# MAGIC %sql
# MAGIC select DISTINCT address, month, MAX(avg_tmpr_c)-MIN(avg_tmpr_c) as tmpr_diff_cmax
# MAGIC from hotel_processed
# MAGIC where month=8
# MAGIC group by address, month
# MAGIC ORDER BY tmpr_diff_cmax DESC

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Get records with max value for each group of grouped SQL results

# COMMAND ----------

# MAGIC %sql
# MAGIC select t1.address, t1.month, t1.avg_tmpr_c, t1.wthr_date
# MAGIC from hotel_processed t1 join hotel_processed t2 on t1.month=t2.month and t2.avg_tmpr_c >= t1.avg_tmpr_c
# MAGIC group by t1.address, t1.month, t1.avg_tmpr_c, t1.wthr_date
# MAGIC having count(*)<=10
# MAGIC order by t1.month

# COMMAND ----------

# MAGIC %sql
# MAGIC select address, month, avg_tmpr_c, wthr_date from
# MAGIC (select hotel_processed.*,
# MAGIC row_number() over (partition by month order by avg_tmpr_c desc) i
# MAGIC from hotel_processed) t where i <= 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC WITH hotel_processed_diff as 
# MAGIC (select DISTINCT address, month, MAX(avg_tmpr_c)-MIN(avg_tmpr_c) as tmpr_diff_cmax
# MAGIC from hotel_processed
# MAGIC group by address, month)
# MAGIC select address, month, tmpr_diff_cmax from
# MAGIC (select hotel_processed_diff.*,
# MAGIC row_number() over (partition by month order by tmpr_diff_cmax desc) i
# MAGIC from hotel_processed_diff) t where i <= 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM (
# MAGIC   (
# MAGIC     (SELECT * FROM hotel_processed WHERE month=8 ORDER BY avg_tmpr_c DESC LIMIT 10)
# MAGIC     UNION ALL (SELECT * FROM hotel_processed WHERE month=9 ORDER BY avg_tmpr_c DESC LIMIT 10)
# MAGIC   )
# MAGIC   UNION ALL (SELECT * FROM hotel_processed WHERE month=10 ORDER BY avg_tmpr_c DESC LIMIT 10)
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select DISTINCT address, month, MAX(avg_tmpr_c) as avg_tmpr_cmax from hotel_processed group by address, month ORDER BY month, avg_tmpr_cmax DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT month, COUNT(month), MAX(avg_tmpr_c) FROM hotel_processed group by month ORDER BY month DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Result

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC WITH hotel_processed_diff as 
# MAGIC (
# MAGIC   select DISTINCT address, month, round(MAX(avg_tmpr_c)-MIN(avg_tmpr_c), 5) as tmpr_diff_cmax
# MAGIC   from hotel_processed
# MAGIC   group by address, month
# MAGIC )
# MAGIC select address, month, tmpr_diff_cmax from
# MAGIC (select hotel_processed_diff.*, row_number() over (partition by month order by tmpr_diff_cmax desc) i
# MAGIC from hotel_processed_diff) t where i <= 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC WITH hotel_processed_diff as 
# MAGIC (
# MAGIC   select DISTINCT address, month, round(MAX(avg_tmpr_c)-MIN(avg_tmpr_c), 5) as tmpr_diff_cmax
# MAGIC   from hotel_processed
# MAGIC   group by address, month
# MAGIC )
# MAGIC select address, month, tmpr_diff_cmax from
# MAGIC (select hotel_processed_diff.*, row_number() over (partition by month order by tmpr_diff_cmax desc) i
# MAGIC from hotel_processed_diff) t where i <= 10;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Top 10 busy (e.g., with the biggest visits count) hotels for each month. If visit dates refer to several months, it should be counted for all affected months.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Analyse

# COMMAND ----------

# MAGIC %sql
# MAGIC select DISTINCT address, COUNT(address) as visitors, month from hotel_processed group by address, month ORDER BY visitors DESC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC desc hotel_processed

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select address, COUNT(address) as visitors
# MAGIC from hotel_processed
# MAGIC where month=8
# MAGIC group by address
# MAGIC ORDER BY visitors DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select day, address, COUNT(address) as visitors
# MAGIC from hotel_processed
# MAGIC where hotel_processed.month=8
# MAGIC group by day, address
# MAGIC ORDER BY visitors DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Top 10 by the number of measurements near this hotel for a given month

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM (
# MAGIC (
# MAGIC (select month, address, COUNT(address) as visitors from hotel_processed where month=8 group by month, address ORDER BY visitors DESC LIMIT 10) UNION ALL 
# MAGIC (select month, address, COUNT(address) as visitors from hotel_processed where month=9 group by month, address ORDER BY visitors DESC LIMIT 10)
# MAGIC )
# MAGIC UNION ALL
# MAGIC (select month, address, COUNT(address) as visitors from hotel_processed where month=10 group by month, address ORDER BY visitors DESC LIMIT 10)
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select month, address, COUNT(address) as visitors from hotel_processed where month=8 group by month, address ORDER BY visitors DESC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Analyse continue...

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from hotel_processed
# MAGIC inner join expedia_processed
# MAGIC on expedia_processed.hotel_id=hotel_processed.id

# COMMAND ----------

# MAGIC %sql
# MAGIC desc expedia_processed

# COMMAND ----------

# MAGIC %sql
# MAGIC select h.address, COUNT(h.address) as visitors
# MAGIC from (
# MAGIC   select *
# MAGIC   from hotel_processed
# MAGIC   inner join expedia_processed
# MAGIC   on expedia_processed.hotel_id=hotel_processed.id
# MAGIC ) h
# MAGIC where month(h.srch_ci)=8 or month(h.srch_co)=8
# MAGIC group by h.address
# MAGIC ORDER BY visitors DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select h.address, COUNT(h.address) as visitors
# MAGIC from (
# MAGIC   select *
# MAGIC   from hotel_processed
# MAGIC   inner join expedia_processed
# MAGIC   on expedia_processed.hotel_id=hotel_processed.id
# MAGIC ) h
# MAGIC where (month(h.srch_ci)=8 or month(h.srch_co)=8) and h.address='Americas Best Value Inn'
# MAGIC group by h.address

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from hotel_processed
# MAGIC inner join expedia_processed
# MAGIC on expedia_processed.hotel_id=hotel_processed.id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT month(srch_ci)
# MAGIC from
# MAGIC (
# MAGIC   select *
# MAGIC   from hotel_processed
# MAGIC   inner join expedia_processed
# MAGIC   on expedia_processed.hotel_id=hotel_processed.id
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Top 10 hotels by number of visits based on inner join

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 
# MAGIC (((
# MAGIC   select month(h.srch_ci) as month_ci, month(h.srch_co) as month_co, h.address, COUNT(h.address) as visitors
# MAGIC   from (
# MAGIC     select *
# MAGIC     from hotel_processed
# MAGIC     inner join expedia_processed
# MAGIC     on expedia_processed.hotel_id=hotel_processed.id
# MAGIC   ) h
# MAGIC   where month(h.srch_ci)=8 or month(h.srch_co)=8
# MAGIC   group by month_ci, month_co, h.address
# MAGIC   ORDER BY visitors DESC
# MAGIC   LIMIT 10
# MAGIC )
# MAGIC UNION ALL
# MAGIC (
# MAGIC   select month(h.srch_ci) as month_ci, month(h.srch_co) as month_co, h.address, COUNT(h.address) as visitors
# MAGIC   from (
# MAGIC     select *
# MAGIC     from hotel_processed
# MAGIC     inner join expedia_processed
# MAGIC     on expedia_processed.hotel_id=hotel_processed.id
# MAGIC   ) h
# MAGIC   where month(h.srch_ci)=9 or month(h.srch_co)=9
# MAGIC   group by month_ci, month_co, h.address
# MAGIC   ORDER BY visitors DESC
# MAGIC   LIMIT 10
# MAGIC ))
# MAGIC UNION ALL
# MAGIC (
# MAGIC   select month(h.srch_ci) as month_ci, month(h.srch_co) as month_co, h.address, COUNT(h.address) as visitors
# MAGIC   from (
# MAGIC     select *
# MAGIC     from hotel_processed
# MAGIC     inner join expedia_processed
# MAGIC     on expedia_processed.hotel_id=hotel_processed.id
# MAGIC   ) h
# MAGIC   where month(h.srch_ci)=10 or month(h.srch_co)=10
# MAGIC   group by month_ci, month_co, h.address
# MAGIC   ORDER BY visitors DESC
# MAGIC   LIMIT 10
# MAGIC ))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Top 10 hotels by number of visits based on 1 expedia table

# COMMAND ----------

# MAGIC %sql
# MAGIC select month(srch_ci) as month_ci, month(srch_co) as month_co, hotel_id, COUNT(hotel_id) as visitors
# MAGIC from expedia_processed
# MAGIC where month(srch_ci)=10 or month(srch_co)=10
# MAGIC group by month_ci, month_co, hotel_id
# MAGIC ORDER BY visitors DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 
# MAGIC (((
# MAGIC   select month(srch_ci) as month_ci, month(srch_co) as month_co, hotel_id, COUNT(hotel_id) as visitors
# MAGIC from expedia_processed
# MAGIC where month(srch_ci)=10 or month(srch_co)=10
# MAGIC group by month_ci, month_co, hotel_id
# MAGIC ORDER BY visitors DESC
# MAGIC LIMIT 10
# MAGIC )
# MAGIC UNION ALL
# MAGIC (
# MAGIC   select month(srch_ci) as month_ci, month(srch_co) as month_co, hotel_id, COUNT(hotel_id) as visitors
# MAGIC   from expedia_processed
# MAGIC   where month(srch_ci)=9 or month(srch_co)=9
# MAGIC   group by month_ci, month_co, hotel_id
# MAGIC   ORDER BY visitors DESC
# MAGIC   LIMIT 10
# MAGIC ))
# MAGIC UNION ALL
# MAGIC (
# MAGIC   select month(srch_ci) as month_ci, month(srch_co) as month_co, hotel_id, COUNT(hotel_id) as visitors
# MAGIC   from expedia_processed
# MAGIC   where month(srch_ci)=8 or month(srch_co)=8
# MAGIC   group by month_ci, month_co, hotel_id
# MAGIC   ORDER BY visitors DESC
# MAGIC   LIMIT 10
# MAGIC ))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Top 10 hotels by the number of people visited for each month

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from hotel_processed
# MAGIC inner join expedia_processed
# MAGIC on expedia_processed.hotel_id=hotel_processed.id

# COMMAND ----------

# MAGIC %sql
# MAGIC select month(h.srch_ci) as month_ci, month(h.srch_co) as month_co, h.address, (h.srch_adults_cnt+h.srch_children_cnt) as visitors
# MAGIC   from (
# MAGIC     select *
# MAGIC     from hotel_processed
# MAGIC     inner join expedia_processed
# MAGIC     on expedia_processed.hotel_id=hotel_processed.id
# MAGIC   ) h
# MAGIC   where (month(h.srch_ci)=8 or month(h.srch_co)=8) and (srch_adults_cnt>0 or srch_children_cnt>0)
# MAGIC   group by month_ci, month_co, h.address, h.srch_adults_cnt, h.srch_children_cnt
# MAGIC   ORDER BY visitors DESC
# MAGIC   LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM 
# MAGIC (((
# MAGIC   select month(h.srch_ci) as month_ci, month(h.srch_co) as month_co, h.address, (h.srch_adults_cnt+h.srch_children_cnt) as visitors
# MAGIC   from (
# MAGIC     select *
# MAGIC     from hotel_processed
# MAGIC     inner join expedia_processed
# MAGIC     on expedia_processed.hotel_id=hotel_processed.id
# MAGIC   ) h
# MAGIC   where (month(h.srch_ci)=8 or month(h.srch_co)=8) and (srch_adults_cnt>0 or srch_children_cnt>0)
# MAGIC   group by month_ci, month_co, h.address, h.srch_adults_cnt, h.srch_children_cnt
# MAGIC   ORDER BY visitors DESC
# MAGIC   LIMIT 10
# MAGIC )
# MAGIC UNION ALL
# MAGIC (
# MAGIC   select month(h.srch_ci) as month_ci, month(h.srch_co) as month_co, h.address, (h.srch_adults_cnt+h.srch_children_cnt) as visitors
# MAGIC   from (
# MAGIC     select *
# MAGIC     from hotel_processed
# MAGIC     inner join expedia_processed
# MAGIC     on expedia_processed.hotel_id=hotel_processed.id
# MAGIC   ) h
# MAGIC   where (month(h.srch_ci)=9 or month(h.srch_co)=9) and (srch_adults_cnt>0 or srch_children_cnt>0)
# MAGIC   group by month_ci, month_co, h.address, h.srch_adults_cnt, h.srch_children_cnt
# MAGIC   ORDER BY visitors DESC
# MAGIC   LIMIT 10
# MAGIC ))
# MAGIC UNION ALL
# MAGIC (
# MAGIC   select month(h.srch_ci) as month_ci, month(h.srch_co) as month_co, h.address, (h.srch_adults_cnt+h.srch_children_cnt) as visitors
# MAGIC   from (
# MAGIC     select *
# MAGIC     from hotel_processed
# MAGIC     inner join expedia_processed
# MAGIC     on expedia_processed.hotel_id=hotel_processed.id
# MAGIC   ) h
# MAGIC   where (month(h.srch_ci)=10 or month(h.srch_co)=10) and (srch_adults_cnt>0 or srch_children_cnt>0)
# MAGIC   group by month_ci, month_co, h.address, h.srch_adults_cnt, h.srch_children_cnt
# MAGIC   ORDER BY visitors DESC
# MAGIC   LIMIT 10
# MAGIC ))

# COMMAND ----------

# MAGIC %sql
# MAGIC select month(h.srch_ci) as month_ci, month(h.srch_co) as month_co, h.address, COUNT(h.address) as visitors
# MAGIC from (
# MAGIC   select *
# MAGIC   from hotel_processed
# MAGIC   inner join expedia_processed
# MAGIC   on expedia_processed.hotel_id=hotel_processed.id
# MAGIC ) h
# MAGIC where month(h.srch_ci)=8 or month(h.srch_co)=8
# MAGIC group by month_ci, month_co, h.address
# MAGIC ORDER BY visitors DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select month(h.srch_ci) as month_ci, month(h.srch_co) as month_co, h.address, COUNT(h.address) as visitors
# MAGIC from (
# MAGIC   select *
# MAGIC   from hotel_processed
# MAGIC   inner join expedia_processed
# MAGIC   on expedia_processed.hotel_id=hotel_processed.id
# MAGIC ) h
# MAGIC where month(h.srch_ci)=8 and month(h.srch_co)=9
# MAGIC group by month_ci, month_co, h.address
# MAGIC ORDER BY visitors DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Get records with max value for each group of grouped SQL results

# COMMAND ----------

# MAGIC %sql
# MAGIC with expedia_processed_count as 
# MAGIC (
# MAGIC   select srch_ci, srch_co, month(srch_ci) as month_ci, month(srch_co) as month_co, hotel_id, COUNT(hotel_id) as visitors
# MAGIC   from expedia_processed
# MAGIC   group by srch_ci, srch_co, month_ci, month_co, hotel_id
# MAGIC )
# MAGIC select srch_ci, srch_co, month_ci, month_co, hotel_id, visitors from
# MAGIC (select expedia_processed_count.*,
# MAGIC row_number() over (partition by month_ci order by visitors desc) i
# MAGIC from expedia_processed_count) t where i <= 10 and srch_ci is not null;

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.srch_ci, a.srch_co, a.hotel_id, b.address
# MAGIC from expedia_processed a
# MAGIC left outer join hotel_processed b on a.hotel_id=b.id
# MAGIC group by a.srch_ci, a.srch_co, a.hotel_id, b.address;

# COMMAND ----------

# MAGIC %sql
# MAGIC select d.srch_ci, d.srch_co, month(d.srch_ci) as month_ci, month(d.srch_co) as month_co, d.hotel_id, d.address, visits
# MAGIC   from (
# MAGIC     select a.srch_ci, a.srch_co, a.hotel_id, b.address, count(a.hotel_id) as visits
# MAGIC     from expedia_processed a
# MAGIC     left outer join hotel_processed b on a.hotel_id=b.id
# MAGIC     group by a.srch_ci, a.srch_co, a.hotel_id, b.address
# MAGIC   ) d
# MAGIC group by d.srch_ci, d.srch_co, month_ci, month_co, d.hotel_id, d.address, visits

# COMMAND ----------

# MAGIC %sql
# MAGIC with expedia_processed_count as
# MAGIC (
# MAGIC   select d.srch_ci, d.srch_co, month(d.srch_ci) as month_ci, month(d.srch_co) as month_co, d.hotel_id, d.address, visits
# MAGIC     from (
# MAGIC       select a.srch_ci, a.srch_co, a.hotel_id, b.address, count(a.hotel_id) as visits
# MAGIC       from expedia_processed a
# MAGIC       left outer join hotel_processed b on a.hotel_id=b.id
# MAGIC       group by a.srch_ci, a.srch_co, a.hotel_id, b.address
# MAGIC     ) d
# MAGIC   group by d.srch_ci, d.srch_co, month_ci, month_co, d.hotel_id, d.address, visits
# MAGIC )
# MAGIC select srch_ci, srch_co, month_ci, month_co, hotel_id, address, visits from
# MAGIC (select expedia_processed_count.*,
# MAGIC row_number() over (partition by month_ci order by visits desc) i
# MAGIC from expedia_processed_count) t where i <= 10 and srch_ci is not null;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Above result is not correctly because the difference between ci and co can be several month

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from
# MAGIC (
# MAGIC   select a.srch_ci, a.srch_co, datediff(a.srch_co, a.srch_ci) as datediff, a.hotel_id
# MAGIC   from expedia_processed a
# MAGIC ) d
# MAGIC where d.datediff>365

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from
# MAGIC (
# MAGIC   select a.srch_ci, a.srch_co, round(months_between(a.srch_co, a.srch_ci), 2) as monthdiff, a.hotel_id
# MAGIC   from expedia_processed a
# MAGIC ) d

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Extract month from srch_co and srch_ci using posexplode and dataframe API and saving temp dataframe to delta table

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

diffMonthsDF = expedia_processed.withColumn("diffMonths", f.round(f.months_between('srch_co', 'srch_ci'), 0))
display(diffMonthsDF)

# COMMAND ----------

demoDF = diffMonthsDF.select("*").where(f.col("diffMonths")==16)
display(demoDF)

display(
  demoDF.withColumn("repeat", f.expr("split(repeat(',', diffMonths), ',')"))\
  .select("*", f.posexplode("repeat").alias("DtMonth", "val"))\
  .drop("repeat", "val", "diffMonths")\
  .withColumn("DtMonth", f.expr("add_months(srch_ci, DtMonth)"))
)

# COMMAND ----------

diffMonthsDFtrasformed = diffMonthsDF.withColumn("repeat", f.expr("split(repeat(',', diffMonths), ',')"))\
                                      .select("*", f.posexplode("repeat").alias("DtMonth", "val"))\
                                      .drop("repeat", "val", "diffMonths")\
                                      .withColumn("dtMonth", f.expr("add_months(srch_ci, dtMonth)"))

display(diffMonthsDFtrasformed)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Create parquet table after extract data from expedia

# COMMAND ----------

path_expedia = "/db/expedia/"

# COMMAND ----------

dbutils.fs.rm(path_expedia + "transformed", recurse=True)

spark.sql(
    f"""
DROP TABLE IF EXISTS expedia_transformed
"""
)

# COMMAND ----------

(
    diffMonthsDFtrasformed.write.mode("overwrite")
    .format("parquet")
    .save(path_expedia + "transformed")
)

# COMMAND ----------

spark.sql(
    f"""
DROP TABLE IF EXISTS expedia_transformed
"""
)
 
spark.sql(
    f"""
CREATE TABLE expedia_transformed
USING PARQUET
LOCATION "{path_expedia}/transformed"
"""
)

# COMMAND ----------

expedia_transformed = spark.read.table("expedia_transformed")
expedia_transformed.count()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Create delta table

# COMMAND ----------

parquet_table = f"parquet.`{path_expedia}transformed`"

DeltaTable.convertToDelta(spark, parquet_table)

# COMMAND ----------

spark.sql(f"""
DROP TABLE IF EXISTS expedia_transformed
""")

spark.sql(f"""
CREATE TABLE expedia_transformed
USING DELTA
LOCATION "{path_expedia}/transformed" 
""")

# COMMAND ----------

expedia_transformed = spark.read.table("expedia_transformed")
expedia_transformed.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### grouping transformed expedia

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from expedia_transformed

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.srch_ci, a.srch_co, a.dtMonth, a.hotel_id, b.address
# MAGIC from expedia_transformed a
# MAGIC inner join hotel_processed b on a.hotel_id=b.id
# MAGIC group by a.srch_ci, a.srch_co, a.dtMonth, a.hotel_id, b.address

# COMMAND ----------

# MAGIC %sql
# MAGIC select year(d.dtMonth) as year, month(d.dtMonth) as month, d.hotel_id, d.address, d.visits
# MAGIC   from (
# MAGIC     select a.srch_ci, a.srch_co, a.dtMonth, a.hotel_id, b.address, count(a.hotel_id) as visits
# MAGIC     from expedia_transformed a
# MAGIC     inner join hotel_processed b on a.hotel_id=b.id
# MAGIC     group by a.srch_ci, a.srch_co, a.dtMonth, a.hotel_id, b.address
# MAGIC   ) d
# MAGIC group by year, month, d.hotel_id, d.address, visits

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Extract month from srch_co and srch_ci using posexplode and SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select d.srch_ci, d.srch_co, d.diffMonths, d.hotel_id, split(repeat(',', diffMonths), ',') as repeat
# MAGIC from
# MAGIC (
# MAGIC   select a.srch_ci, a.srch_co, round(months_between(a.srch_co, a.srch_ci), 0) as diffMonths, a.hotel_id
# MAGIC   from expedia_processed a
# MAGIC ) d
# MAGIC where d.diffMonths=16

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Create dataframe with explode

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select c.srch_ci, c.srch_co, c.diffMonths, c.hotel_id, add_months(c.srch_ci, c.pos) as dtMonth
# MAGIC from
# MAGIC (
# MAGIC   select *, posexplode(a.repeat)
# MAGIC   from
# MAGIC   (
# MAGIC     select d.srch_ci, d.srch_co, d.diffMonths, d.hotel_id, split(repeat(',', diffMonths), ',') as repeat
# MAGIC     from
# MAGIC     (
# MAGIC       select a.srch_ci, a.srch_co, round(months_between(a.srch_co, a.srch_ci), 0) as diffMonths, a.hotel_id
# MAGIC       from expedia_processed a
# MAGIC     ) d
# MAGIC   ) a
# MAGIC ) c

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Result (external saving)

# COMMAND ----------

# MAGIC %sql
# MAGIC with expedia_transformed_count as
# MAGIC (
# MAGIC   select year(d.dtMonth) as year, month(d.dtMonth) as month, d.hotel_id, d.address, d.visits
# MAGIC     from (
# MAGIC       select a.srch_ci, a.srch_co, a.dtMonth, a.hotel_id, b.address, count(a.hotel_id) as visits
# MAGIC       from expedia_transformed a
# MAGIC       inner join hotel_processed b on a.hotel_id=b.id
# MAGIC       group by a.srch_ci, a.srch_co, a.dtMonth, a.hotel_id, b.address
# MAGIC     ) d
# MAGIC   group by year, month, d.hotel_id, d.address, visits
# MAGIC )
# MAGIC select year, month, hotel_id, address, visits from
# MAGIC (select expedia_transformed_count.*,
# MAGIC row_number() over (partition by year, month order by visits desc) i
# MAGIC from expedia_transformed_count) t where i <= 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC with expedia_transformed_count as
# MAGIC (
# MAGIC   select year(d.dtMonth) as year, month(d.dtMonth) as month, d.hotel_id, d.address, d.visits
# MAGIC     from (
# MAGIC       select a.srch_ci, a.srch_co, a.dtMonth, a.hotel_id, b.address, count(a.hotel_id) as visits
# MAGIC       from expedia_transformed a
# MAGIC       inner join hotel_processed b on a.hotel_id=b.id
# MAGIC       group by a.srch_ci, a.srch_co, a.dtMonth, a.hotel_id, b.address
# MAGIC     ) d
# MAGIC   group by year, month, d.hotel_id, d.address, visits
# MAGIC )
# MAGIC select year, month, hotel_id, address, visits from
# MAGIC (select expedia_transformed_count.*,
# MAGIC row_number() over (partition by year, month order by visits desc) i
# MAGIC from expedia_transformed_count) t where i <= 10;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Result

# COMMAND ----------

# MAGIC %sql
# MAGIC with 
# MAGIC expedia_processed_transformed as
# MAGIC (
# MAGIC   select c.srch_ci, c.srch_co, c.diffMonths, c.hotel_id, add_months(c.srch_ci, c.pos) as dtMonth
# MAGIC   from
# MAGIC   (
# MAGIC     select *, posexplode(a.repeat)
# MAGIC     from
# MAGIC     (
# MAGIC       select d.srch_ci, d.srch_co, d.diffMonths, d.hotel_id, split(repeat(',', diffMonths), ',') as repeat
# MAGIC       from
# MAGIC       (
# MAGIC         select a.srch_ci, a.srch_co, round(months_between(a.srch_co, a.srch_ci), 0) as diffMonths, a.hotel_id
# MAGIC         from expedia_processed a
# MAGIC       ) d
# MAGIC     ) a
# MAGIC   ) c
# MAGIC ),
# MAGIC expedia_transformed_count as
# MAGIC (
# MAGIC   select year(d.dtMonth) as year, month(d.dtMonth) as month, d.hotel_id, d.address, d.visits
# MAGIC     from (
# MAGIC       select a.srch_ci, a.srch_co, a.dtMonth, a.hotel_id, b.address, count(a.hotel_id) as visits
# MAGIC       from expedia_processed_transformed a
# MAGIC       inner join hotel_processed b on a.hotel_id=b.id
# MAGIC       group by a.srch_ci, a.srch_co, a.dtMonth, a.hotel_id, b.address
# MAGIC     ) d
# MAGIC   group by year, month, d.hotel_id, d.address, visits
# MAGIC )
# MAGIC select year, month, hotel_id, address, visits from
# MAGIC (select expedia_transformed_count.*,
# MAGIC row_number() over (partition by year, month order by visits desc) i
# MAGIC from expedia_transformed_count) t where i <= 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC with 
# MAGIC expedia_processed_transformed as
# MAGIC (
# MAGIC   select c.srch_ci, c.srch_co, c.diffMonths, c.hotel_id, add_months(c.srch_ci, c.pos) as dtMonth
# MAGIC   from
# MAGIC   (
# MAGIC     select *, posexplode(a.repeat)
# MAGIC     from
# MAGIC     (
# MAGIC       select d.srch_ci, d.srch_co, d.diffMonths, d.hotel_id, split(repeat(',', diffMonths), ',') as repeat
# MAGIC       from
# MAGIC       (
# MAGIC         select a.srch_ci, a.srch_co, round(months_between(a.srch_co, a.srch_ci), 0) as diffMonths, a.hotel_id
# MAGIC         from expedia_processed a
# MAGIC       ) d
# MAGIC     ) a
# MAGIC   ) c
# MAGIC ),
# MAGIC expedia_transformed_count as
# MAGIC (
# MAGIC   select year(d.dtMonth) as year, month(d.dtMonth) as month, d.hotel_id, d.address, d.visits
# MAGIC     from (
# MAGIC       select a.srch_ci, a.srch_co, a.dtMonth, a.hotel_id, b.address, count(a.hotel_id) as visits
# MAGIC       from expedia_processed_transformed a
# MAGIC       inner join hotel_processed b on a.hotel_id=b.id
# MAGIC       group by a.srch_ci, a.srch_co, a.dtMonth, a.hotel_id, b.address
# MAGIC     ) d
# MAGIC   group by year, month, d.hotel_id, d.address, visits
# MAGIC )
# MAGIC select year, month, hotel_id, address, visits from
# MAGIC (select expedia_transformed_count.*,
# MAGIC row_number() over (partition by year, month order by visits desc) i
# MAGIC from expedia_transformed_count) t where i <= 10;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### For visits with extended stay (more than 7 days) calculate weather trend (the day temperature difference between last and first day of stay) and average temperature during stay.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Analyse

# COMMAND ----------

# MAGIC %sql
# MAGIC select hotel_id, DATEDIFF(srch_co, srch_ci) as days
# MAGIC from expedia_processed

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct address, avg_tmpr_c, wthr_date, srch_ci, srch_co, city, country, geoHash, year, month
# MAGIC from hotel_processed
# MAGIC inner join expedia_processed
# MAGIC on expedia_processed.hotel_id=hotel_processed.id
# MAGIC where abs(day(srch_co)-day(srch_ci))>=7 and wthr_date between srch_ci and srch_co

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct address, avg_tmpr_c, wthr_date, srch_ci, srch_co, city, country, geoHash, year, month
# MAGIC from hotel_processed
# MAGIC inner join expedia_processed
# MAGIC on expedia_processed.hotel_id=hotel_processed.id
# MAGIC where abs(day(srch_co)-day(srch_ci))>=7 and wthr_date between srch_ci and srch_co

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from hotel_processed
# MAGIC inner join expedia_processed
# MAGIC on expedia_processed.hotel_id=hotel_processed.id
# MAGIC where abs(day(srch_co)-day(srch_ci))>=7 and wthr_date between srch_ci and srch_co

# COMMAND ----------

# MAGIC %sql
# MAGIC select h.*
# MAGIC   from (
# MAGIC     select *
# MAGIC     from hotel_processed
# MAGIC     inner join expedia_processed
# MAGIC     on expedia_processed.hotel_id=hotel_processed.id
# MAGIC     where abs(day(srch_co)-day(srch_ci))>=7
# MAGIC   ) h
# MAGIC where h.wthr_date=h.srch_ci or h.wthr_date=h.srch_co or h.wthr_date between h.srch_co and h.srch_ci

# COMMAND ----------

# MAGIC %sql
# MAGIC select d.address, d.avg_tmpr_c, d.wthr_date, d.srch_ci, d.srch_co,
# MAGIC CASE WHEN d.wthr_date=d.srch_ci THEN d.avg_tmpr_c ELSE NULL END as avg_tmpr_c_ci,
# MAGIC CASE WHEN d.wthr_date=d.srch_co THEN d.avg_tmpr_c ELSE NULL END as avg_tmpr_c_co
# MAGIC from
# MAGIC (
# MAGIC   select h.*
# MAGIC   from (
# MAGIC     select *
# MAGIC     from hotel_processed
# MAGIC     inner join expedia_processed
# MAGIC     on expedia_processed.hotel_id=hotel_processed.id
# MAGIC     where abs(day(srch_co)-day(srch_ci))>=7
# MAGIC   ) h
# MAGIC   where h.wthr_date=h.srch_ci or h.wthr_date=h.srch_co or h.wthr_date between h.srch_co and h.srch_ci
# MAGIC ) d

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Let's calculate data separatly and then join them

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Save data for check in avg temprature in new column

# COMMAND ----------

# MAGIC %sql
# MAGIC select d.address, d.avg_tmpr_c, d.wthr_date, d.srch_ci, d.srch_co,
# MAGIC CASE WHEN d.wthr_date=d.srch_ci THEN d.avg_tmpr_c ELSE NULL END as avg_tmpr_c_ci
# MAGIC from
# MAGIC (
# MAGIC   select h.*
# MAGIC   from (
# MAGIC     select *
# MAGIC     from hotel_processed
# MAGIC     inner join expedia_processed
# MAGIC     on expedia_processed.hotel_id=hotel_processed.id
# MAGIC     where abs(day(srch_co)-day(srch_ci))>=7
# MAGIC   ) h
# MAGIC   where h.wthr_date=h.srch_ci
# MAGIC ) d

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Save data for check out avg temprature in new column

# COMMAND ----------

# MAGIC %sql
# MAGIC select d.address, d.avg_tmpr_c, d.wthr_date, d.srch_ci, d.srch_co,
# MAGIC CASE WHEN d.wthr_date=d.srch_co THEN d.avg_tmpr_c ELSE NULL END as avg_tmpr_c_co
# MAGIC from
# MAGIC (
# MAGIC   select h.*
# MAGIC   from (
# MAGIC     select *
# MAGIC     from hotel_processed
# MAGIC     inner join expedia_processed
# MAGIC     on expedia_processed.hotel_id=hotel_processed.id
# MAGIC     where abs(day(srch_co)-day(srch_ci))>=7
# MAGIC   ) h
# MAGIC   where h.wthr_date=h.srch_co
# MAGIC ) d

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Join the splited parts

# COMMAND ----------

# MAGIC %sql
# MAGIC select da.address, da.avg_tmpr_c, da.wthr_date, da.srch_ci, da.srch_co, da.avg_tmpr_c_co, db.avg_tmpr_c_ci
# MAGIC from
# MAGIC (
# MAGIC   select d.address, d.avg_tmpr_c, d.wthr_date, d.srch_ci, d.srch_co,
# MAGIC   CASE WHEN d.wthr_date=d.srch_co THEN d.avg_tmpr_c ELSE NULL END as avg_tmpr_c_co
# MAGIC   from
# MAGIC   (
# MAGIC     select h.*
# MAGIC     from (
# MAGIC       select *
# MAGIC       from hotel_processed
# MAGIC       inner join expedia_processed
# MAGIC       on expedia_processed.hotel_id=hotel_processed.id
# MAGIC       where abs(day(srch_co)-day(srch_ci))>=7
# MAGIC     ) h
# MAGIC     where h.wthr_date=h.srch_co
# MAGIC   ) d
# MAGIC ) da
# MAGIC inner join
# MAGIC (
# MAGIC   select d.address, d.avg_tmpr_c, d.wthr_date, d.srch_ci, d.srch_co,
# MAGIC   CASE WHEN d.wthr_date=d.srch_ci THEN d.avg_tmpr_c ELSE NULL END as avg_tmpr_c_ci
# MAGIC   from
# MAGIC   (
# MAGIC     select h.*
# MAGIC     from (
# MAGIC       select *
# MAGIC       from hotel_processed
# MAGIC       inner join expedia_processed
# MAGIC       on expedia_processed.hotel_id=hotel_processed.id
# MAGIC       where abs(day(srch_co)-day(srch_ci))>=7
# MAGIC     ) h
# MAGIC     where h.wthr_date=h.srch_ci
# MAGIC   ) d
# MAGIC ) db
# MAGIC on (da.srch_ci = db.srch_ci and da.address = db.address)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Result

# COMMAND ----------

# MAGIC %sql
# MAGIC select da.address, da.avg_tmpr_c, da.wthr_date, da.srch_ci, da.srch_co, da.avg_tmpr_c_co, db.avg_tmpr_c_ci, round((abs(da.avg_tmpr_c_co-db.avg_tmpr_c_ci)), 5) as weather_trend, round(((da.avg_tmpr_c_co+db.avg_tmpr_c_ci)/2), 5) as avg_tmpr_c_visits
# MAGIC from
# MAGIC (
# MAGIC   select d.address, d.avg_tmpr_c, d.wthr_date, d.srch_ci, d.srch_co,
# MAGIC   CASE WHEN d.wthr_date=d.srch_co THEN d.avg_tmpr_c ELSE NULL END as avg_tmpr_c_co
# MAGIC   from
# MAGIC   (
# MAGIC     select h.*
# MAGIC     from (
# MAGIC       select *
# MAGIC       from hotel_processed
# MAGIC       inner join expedia_processed
# MAGIC       on expedia_processed.hotel_id=hotel_processed.id
# MAGIC       where abs(day(srch_co)-day(srch_ci))>=7
# MAGIC     ) h
# MAGIC     where h.wthr_date=h.srch_co
# MAGIC   ) d
# MAGIC ) da
# MAGIC inner join
# MAGIC (
# MAGIC   select d.address, d.avg_tmpr_c, d.wthr_date, d.srch_ci, d.srch_co,
# MAGIC   CASE WHEN d.wthr_date=d.srch_ci THEN d.avg_tmpr_c ELSE NULL END as avg_tmpr_c_ci
# MAGIC   from
# MAGIC   (
# MAGIC     select h.*
# MAGIC     from (
# MAGIC       select *
# MAGIC       from hotel_processed
# MAGIC       inner join expedia_processed
# MAGIC       on expedia_processed.hotel_id=hotel_processed.id
# MAGIC       where abs(day(srch_co)-day(srch_ci))>=7
# MAGIC     ) h
# MAGIC     where h.wthr_date=h.srch_ci
# MAGIC   ) d
# MAGIC ) db
# MAGIC on (da.srch_ci = db.srch_ci and da.address = db.address)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Save result in ADLS Storage gen 2

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Save result: task 1

# COMMAND ----------

sqlDF1 = spark.sql("""
WITH hotel_processed_diff as 
(
  select DISTINCT address, month, round(MAX(avg_tmpr_c)-MIN(avg_tmpr_c), 5) as tmpr_diff_cmax
  from hotel_processed
  group by address, month
)
select address, month, tmpr_diff_cmax from
(select hotel_processed_diff.*, row_number() over (partition by month order by tmpr_diff_cmax desc) i
from hotel_processed_diff) t where i <= 10
""")
display(sqlDF1)

# COMMAND ----------

sqlDF1.write.partitionBy("month").parquet(STORAGE_LINK_STRING_ADLS_WRITE + "/result_task1")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Save result: task 2

# COMMAND ----------

sqlDF2 = spark.sql("""
with 
expedia_processed_transformed as
(
  select c.srch_ci, c.srch_co, c.diffMonths, c.hotel_id, add_months(c.srch_ci, c.pos) as dtMonth
  from
  (
    select *, posexplode(a.repeat)
    from
    (
      select d.srch_ci, d.srch_co, d.diffMonths, d.hotel_id, split(repeat(',', diffMonths), ',') as repeat
      from
      (
        select a.srch_ci, a.srch_co, round(months_between(a.srch_co, a.srch_ci), 0) as diffMonths, a.hotel_id
        from expedia_processed a
      ) d
    ) a
  ) c
),
expedia_transformed_count as
(
  select year(d.dtMonth) as year, month(d.dtMonth) as month, d.hotel_id, d.address, d.visits
    from (
      select a.srch_ci, a.srch_co, a.dtMonth, a.hotel_id, b.address, count(a.hotel_id) as visits
      from expedia_processed_transformed a
      inner join hotel_processed b on a.hotel_id=b.id
      group by a.srch_ci, a.srch_co, a.dtMonth, a.hotel_id, b.address
    ) d
  group by year, month, d.hotel_id, d.address, visits
)
select year, month, hotel_id, address, visits from
(select expedia_transformed_count.*,
row_number() over (partition by year, month order by visits desc) i
from expedia_transformed_count) t where i <= 10
""")
display(sqlDF2)

# COMMAND ----------

sqlDF2.write.partitionBy("year", "month").parquet(STORAGE_LINK_STRING_ADLS_WRITE + "/result_task2")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Save result: task 3

# COMMAND ----------

sqlDF3 = spark.sql("""
select da.address, da.avg_tmpr_c, da.wthr_date, da.srch_ci, da.srch_co, da.avg_tmpr_c_co, db.avg_tmpr_c_ci, round((abs(da.avg_tmpr_c_co-db.avg_tmpr_c_ci)), 5) as weather_trend, round(((da.avg_tmpr_c_co+db.avg_tmpr_c_ci)/2), 5) as avg_tmpr_c_visits
from
(
  select d.address, d.avg_tmpr_c, d.wthr_date, d.srch_ci, d.srch_co,
  CASE WHEN d.wthr_date=d.srch_co THEN d.avg_tmpr_c ELSE NULL END as avg_tmpr_c_co
  from
  (
    select h.*
    from (
      select *
      from hotel_processed
      inner join expedia_processed
      on expedia_processed.hotel_id=hotel_processed.id
      where abs(day(srch_co)-day(srch_ci))>=7
    ) h
    where h.wthr_date=h.srch_co
  ) d
) da
inner join
(
  select d.address, d.avg_tmpr_c, d.wthr_date, d.srch_ci, d.srch_co,
  CASE WHEN d.wthr_date=d.srch_ci THEN d.avg_tmpr_c ELSE NULL END as avg_tmpr_c_ci
  from
  (
    select h.*
    from (
      select *
      from hotel_processed
      inner join expedia_processed
      on expedia_processed.hotel_id=hotel_processed.id
      where abs(day(srch_co)-day(srch_ci))>=7
    ) h
    where h.wthr_date=h.srch_ci
  ) d
) db
on (da.srch_ci = db.srch_ci and da.address = db.address)
""")
display(sqlDF3)

# COMMAND ----------

sqlDF3.write.parquet(STORAGE_LINK_STRING_ADLS_WRITE + "/result_task3")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save joined data with all the fields from both datasets

# COMMAND ----------

sqlDFjoined = spark.sql("""
select address, avg_tmpr_c, avg_tmpr_f, city, country, geoHash, latitude, longitude, name, wthr_date, year, month, day, date_time, site_name, posa_continent, user_location_country, user_location_region, user_location_city, orig_destination_distance, 
        user_id, is_mobile, is_package, channel, srch_ci, srch_co, srch_adults_cnt, srch_children_cnt, srch_rm_cnt, srch_destination_id, srch_destination_type_id, hotel_id
from hotel_processed
inner join expedia_processed
on expedia_processed.hotel_id=hotel_processed.id
""")
display(sqlDFjoined)

# COMMAND ----------

sqlDFjoined.write.partitionBy("year", "month", "day").parquet(STORAGE_LINK_STRING_ADLS_WRITE + "/result_joined")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Save temp df results

# COMMAND ----------

sqlDFtemp1 = spark.sql("""
select month(srch_ci) as month_ci, month(srch_co) as month_co, hotel_id, COUNT(hotel_id) as visitors
from expedia_processed
where month(srch_ci)=10 or month(srch_co)=10
group by month_ci, month_co, hotel_id
ORDER BY visitors DESC
LIMIT 10
""")
display(sqlDFtemp1)

# COMMAND ----------

sqlDFtemp1.write.partitionBy("month_ci").parquet(STORAGE_LINK_STRING_ADLS_WRITE + "/result_tempdf_task2")

# COMMAND ----------

sqlDFtemp2 = spark.sql("""
select d.address, d.avg_tmpr_c, d.wthr_date, d.srch_ci, d.srch_co,
CASE WHEN d.wthr_date=d.srch_ci THEN d.avg_tmpr_c ELSE NULL END as avg_tmpr_c_ci,
CASE WHEN d.wthr_date=d.srch_co THEN d.avg_tmpr_c ELSE NULL END as avg_tmpr_c_co
from
(
  select h.*
  from (
    select *
    from hotel_processed
    inner join expedia_processed
    on expedia_processed.hotel_id=hotel_processed.id
    where abs(day(srch_co)-day(srch_ci))>=7
  ) h
  where h.wthr_date=h.srch_ci or h.wthr_date=h.srch_co or h.wthr_date between h.srch_co and h.srch_ci
) d
""")
display(sqlDFtemp2)

# COMMAND ----------

sqlDFtemp2.write.partitionBy("srch_ci", "srch_co").parquet(STORAGE_LINK_STRING_ADLS_WRITE + "/result_temp_df_cico_task3")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution plans

# COMMAND ----------

# MAGIC %md
# MAGIC ### task 1

# COMMAND ----------

sqlDF1.explain()

# COMMAND ----------

sqlDF1.explain(extended=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN CODEGEN
# MAGIC WITH hotel_processed_diff as 
# MAGIC (
# MAGIC   select DISTINCT address, month, round(MAX(avg_tmpr_c)-MIN(avg_tmpr_c), 5) as tmpr_diff_cmax
# MAGIC   from hotel_processed
# MAGIC   group by address, month
# MAGIC )
# MAGIC select address, month, tmpr_diff_cmax from
# MAGIC (select hotel_processed_diff.*, row_number() over (partition by month order by tmpr_diff_cmax desc) i
# MAGIC from hotel_processed_diff) t where i <= 10;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ```
# MAGIC == Optimized Logical Plan ==
# MAGIC Project [address#1638, month#1650, tmpr_diff_cmax#1618], Statistics(sizeInBytes=140.0 KiB)
# MAGIC +- Filter (isnotnull(i#1617) AND (i#1617 <= 10)), Statistics(sizeInBytes=154.0 KiB)
# MAGIC    +- Window [address#1638, month#1650, tmpr_diff_cmax#1618, row_number() windowspecdefinition(month#1650, tmpr_diff_cmax#1618 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS i#1617], [month#1650], [tmpr_diff_cmax#1618 DESC NULLS LAST], Statistics(sizeInBytes=154.0 KiB)
# MAGIC       +- Aggregate [address#1638, month#1650], [address#1638, month#1650, round((max(avg_tmpr_c#1639) - min(avg_tmpr_c#1639)), 5) AS tmpr_diff_cmax#1618], Statistics(sizeInBytes=140.0 KiB)
# MAGIC          +- Project [address#1638, avg_tmpr_c#1639, month#1650], Statistics(sizeInBytes=140.0 KiB)
# MAGIC             +- Relation[address#1638,avg_tmpr_c#1639,avg_tmpr_f#1640,city#1641,country#1642,geoHash#1643,id#1644,latitude#1645,longitude#1646,name#1647,wthr_date#1648,year#1649,month#1650,day#1651] parquet, Statistics(sizeInBytes=672.1 KiB)
# MAGIC 
# MAGIC == Physical Plan ==
# MAGIC AdaptiveSparkPlan isFinalPlan=false
# MAGIC +- Project [address#1638, month#1650, tmpr_diff_cmax#1618]
# MAGIC    +- Filter (isnotnull(i#1617) AND (i#1617 <= 10))
# MAGIC       +- RunningWindowFunction [address#1638, month#1650, tmpr_diff_cmax#1618, row_number() windowspecdefinition(month#1650, tmpr_diff_cmax#1618 DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS i#1617], [month#1650], [tmpr_diff_cmax#1618 DESC NULLS LAST], false
# MAGIC          +- Sort [month#1650 ASC NULLS FIRST, tmpr_diff_cmax#1618 DESC NULLS LAST], false, 0
# MAGIC             +- Exchange hashpartitioning(month#1650, 200), ENSURE_REQUIREMENTS, [id=#2666]
# MAGIC                +- HashAggregate(keys=[address#1638, month#1650], functions=[finalmerge_max(merge max#1656) AS max(avg_tmpr_c#1639)#1652, finalmerge_min(merge min#1658) AS min(avg_tmpr_c#1639)#1653], output=[address#1638, month#1650, tmpr_diff_cmax#1618])
# MAGIC                   +- Exchange hashpartitioning(address#1638, month#1650, 200), ENSURE_REQUIREMENTS, [id=#2663]
# MAGIC                      +- HashAggregate(keys=[address#1638, month#1650], functions=[partial_max(avg_tmpr_c#1639) AS max#1656, partial_min(avg_tmpr_c#1639) AS min#1658], output=[address#1638, month#1650, max#1656, min#1658])
# MAGIC                         +- FileScan parquet db_sparksql.hotel_processed[address#1638,avg_tmpr_c#1639,month#1650] Batched: true, DataFilters: [], Format: Parquet, Location: PreparedDeltaFileIndex[dbfs:/db/hotel-weather/processed], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<address:string,avg_tmpr_c:double,month:int>
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN COST
# MAGIC WITH hotel_processed_diff as 
# MAGIC (
# MAGIC   select DISTINCT address, month, round(MAX(avg_tmpr_c)-MIN(avg_tmpr_c), 5) as tmpr_diff_cmax
# MAGIC   from hotel_processed
# MAGIC   group by address, month
# MAGIC )
# MAGIC select address, month, tmpr_diff_cmax from
# MAGIC (select hotel_processed_diff.*, row_number() over (partition by month order by tmpr_diff_cmax desc) i
# MAGIC from hotel_processed_diff) t where i <= 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2

# COMMAND ----------

sqlDF2.explain()

# COMMAND ----------

sqlDF2.explain(extended=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN CODEGEN
# MAGIC with 
# MAGIC expedia_processed_transformed as
# MAGIC (
# MAGIC   select c.srch_ci, c.srch_co, c.diffMonths, c.hotel_id, add_months(c.srch_ci, c.pos) as dtMonth
# MAGIC   from
# MAGIC   (
# MAGIC     select *, posexplode(a.repeat)
# MAGIC     from
# MAGIC     (
# MAGIC       select d.srch_ci, d.srch_co, d.diffMonths, d.hotel_id, split(repeat(',', diffMonths), ',') as repeat
# MAGIC       from
# MAGIC       (
# MAGIC         select a.srch_ci, a.srch_co, round(months_between(a.srch_co, a.srch_ci), 0) as diffMonths, a.hotel_id
# MAGIC         from expedia_processed a
# MAGIC       ) d
# MAGIC     ) a
# MAGIC   ) c
# MAGIC ),
# MAGIC expedia_transformed_count as
# MAGIC (
# MAGIC   select year(d.dtMonth) as year, month(d.dtMonth) as month, d.hotel_id, d.address, d.visits
# MAGIC     from (
# MAGIC       select a.srch_ci, a.srch_co, a.dtMonth, a.hotel_id, b.address, count(a.hotel_id) as visits
# MAGIC       from expedia_processed_transformed a
# MAGIC       inner join hotel_processed b on a.hotel_id=b.id
# MAGIC       group by a.srch_ci, a.srch_co, a.dtMonth, a.hotel_id, b.address
# MAGIC     ) d
# MAGIC   group by year, month, d.hotel_id, d.address, visits
# MAGIC )
# MAGIC select year, month, hotel_id, address, visits from
# MAGIC (select expedia_transformed_count.*,
# MAGIC row_number() over (partition by year, month order by visits desc) i
# MAGIC from expedia_transformed_count) t where i <= 10

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC == Optimized Logical Plan ==
# MAGIC Project [year#2173, month#2174, hotel_id#2219L, address#2234, visits#2172L], Statistics(sizeInBytes=102.5 GiB)
# MAGIC +- Filter (isnotnull(i#2168) AND (i#2168 <= 10)), Statistics(sizeInBytes=110.4 GiB)
# MAGIC    +- Window [year#2173, month#2174, hotel_id#2219L, address#2234, visits#2172L, row_number() windowspecdefinition(year#2173, month#2174, visits#2172L DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS i#2168], [year#2173, month#2174], [visits#2172L DESC NULLS LAST], Statistics(sizeInBytes=110.4 GiB)
# MAGIC       +- Aggregate [year(dtMonth#2171), month(dtMonth#2171), hotel_id#2219L, address#2234, visits#2172L], [year(dtMonth#2171) AS year#2173, month(dtMonth#2171) AS month#2174, hotel_id#2219L, address#2234, visits#2172L], Statistics(sizeInBytes=102.5 GiB)
# MAGIC          +- Project [dtMonth#2171, hotel_id#2219L, address#2234, count(hotel_id#2219L)#2251L AS visits#2172L], Statistics(sizeInBytes=94.7 GiB)
# MAGIC             +- AggregatePart [srch_ci#2212, srch_co#2213, dtMonth#2171, hotel_id#2219L, address#2234], [finalmerge_count(merge count#2255L) AS count(hotel_id#2219L)#2251L], true, Statistics(sizeInBytes=173.5 GiB)
# MAGIC                +- AggregatePart [srch_ci#2212, srch_co#2213, dtMonth#2171, hotel_id#2219L, address#2234], [merge_count(merge count#2255L) AS count#2255L], false, Statistics(sizeInBytes=173.5 GiB)
# MAGIC                   +- Project [srch_ci#2212, srch_co#2213, dtMonth#2171, hotel_id#2219L, address#2234, count#2255L], Statistics(sizeInBytes=173.5 GiB)
# MAGIC                      +- Join Inner, (hotel_id#2219L = cast(id#2240 as bigint)), Statistics(sizeInBytes=213.0 GiB)
# MAGIC                         :- AggregatePart [srch_ci#2212, srch_co#2213, dtMonth#2171, hotel_id#2219L], [partial_count(hotel_id#2219L) AS count#2255L], false, Statistics(sizeInBytes=38.3 MiB)
# MAGIC                         :  +- Project [srch_ci#2212, srch_co#2213, hotel_id#2219L, add_months(cast(srch_ci#2212 as date), pos#2249) AS dtMonth#2171], Statistics(sizeInBytes=33.8 MiB)
# MAGIC                         :     +- Generate posexplode(repeat#2170), [3], false, [pos#2249, col#2250], Statistics(sizeInBytes=45.1 MiB)
# MAGIC                         :        +- Project [srch_ci#2212, srch_co#2213, hotel_id#2219L, split(repeat(,, cast(round(months_between(cast(srch_co#2213 as timestamp), cast(srch_ci#2212 as timestamp), true, Some(Etc/UTC)), 0) as int)), ,, -1) AS repeat#2170], Statistics(sizeInBytes=42.8 MiB)
# MAGIC                         :           +- Filter (((size(split(repeat(,, cast(round(months_between(cast(srch_co#2213 as timestamp), cast(srch_ci#2212 as timestamp), true, Some(Etc/UTC)), 0) as int)), ,, -1), true) > 0) AND isnotnull(split(repeat(,, cast(round(months_between(cast(srch_co#2213 as timestamp), cast(srch_ci#2212 as timestamp), true, Some(Etc/UTC)), 0) as int)), ,, -1))) AND isnotnull(hotel_id#2219L)), Statistics(sizeInBytes=83.4 MiB)
# MAGIC                         :              +- Relation[id#2200L,date_time#2201,site_name#2202,posa_continent#2203,user_location_country#2204,user_location_region#2205,user_location_city#2206,orig_destination_distance#2207,user_id#2208,is_mobile#2209,is_package#2210,channel#2211,srch_ci#2212,srch_co#2213,srch_adults_cnt#2214,srch_children_cnt#2215,srch_rm_cnt#2216,srch_destination_id#2217,srch_destination_type_id#2218,hotel_id#2219L] parquet, Statistics(sizeInBytes=83.4 MiB)
# MAGIC                         +- Project [address#2234, id#2240], Statistics(sizeInBytes=168.0 KiB)
# MAGIC                            +- Filter isnotnull(id#2240), Statistics(sizeInBytes=672.1 KiB)
# MAGIC                               +- Relation[address#2234,avg_tmpr_c#2235,avg_tmpr_f#2236,city#2237,country#2238,geoHash#2239,id#2240,latitude#2241,longitude#2242,name#2243,wthr_date#2244,year#2245,month#2246,day#2247] parquet, Statistics(sizeInBytes=672.1 KiB)
# MAGIC 
# MAGIC == Physical Plan ==
# MAGIC AdaptiveSparkPlan isFinalPlan=false
# MAGIC +- Project [year#2173, month#2174, hotel_id#2219L, address#2234, visits#2172L]
# MAGIC    +- Filter (isnotnull(i#2168) AND (i#2168 <= 10))
# MAGIC       +- RunningWindowFunction [year#2173, month#2174, hotel_id#2219L, address#2234, visits#2172L, row_number() windowspecdefinition(year#2173, month#2174, visits#2172L DESC NULLS LAST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS i#2168], [year#2173, month#2174], [visits#2172L DESC NULLS LAST], false
# MAGIC          +- Sort [year#2173 ASC NULLS FIRST, month#2174 ASC NULLS FIRST, visits#2172L DESC NULLS LAST], false, 0
# MAGIC             +- Exchange hashpartitioning(year#2173, month#2174, 200), ENSURE_REQUIREMENTS, [id=#3931]
# MAGIC                +- HashAggregate(keys=[year(dtMonth#2171)#2284, month(dtMonth#2171)#2285, hotel_id#2219L, address#2234, visits#2172L], functions=[], output=[year#2173, month#2174, hotel_id#2219L, address#2234, visits#2172L])
# MAGIC                   +- Exchange hashpartitioning(year(dtMonth#2171)#2284, month(dtMonth#2171)#2285, hotel_id#2219L, address#2234, visits#2172L, 200), ENSURE_REQUIREMENTS, [id=#3928]
# MAGIC                      +- HashAggregate(keys=[year(dtMonth#2171) AS year(dtMonth#2171)#2284, month(dtMonth#2171) AS month(dtMonth#2171)#2285, hotel_id#2219L, address#2234, visits#2172L], functions=[], output=[year(dtMonth#2171)#2284, month(dtMonth#2171)#2285, hotel_id#2219L, address#2234, visits#2172L])
# MAGIC                         +- Project [dtMonth#2171, hotel_id#2219L, address#2234, count(hotel_id#2219L)#2251L AS visits#2172L]
# MAGIC                            +- HashAggregate(keys=[srch_ci#2212, srch_co#2213, dtMonth#2171, hotel_id#2219L, address#2234], functions=[finalmerge_count(merge count#2255L) AS count(hotel_id#2219L)#2251L], output=[srch_ci#2212, srch_co#2213, dtMonth#2171, hotel_id#2219L, address#2234, count(hotel_id#2219L)#2251L])
# MAGIC                               +- Exchange hashpartitioning(srch_ci#2212, srch_co#2213, dtMonth#2171, hotel_id#2219L, address#2234, 200), ENSURE_REQUIREMENTS, [id=#3923]
# MAGIC                                  +- HashAggregate(keys=[srch_ci#2212, srch_co#2213, dtMonth#2171, hotel_id#2219L, address#2234], functions=[merge_count(merge count#2255L) AS count#2255L], output=[srch_ci#2212, srch_co#2213, dtMonth#2171, hotel_id#2219L, address#2234, count#2255L])
# MAGIC                                     +- Project [srch_ci#2212, srch_co#2213, dtMonth#2171, hotel_id#2219L, address#2234, count#2255L]
# MAGIC                                        +- BroadcastHashJoin [hotel_id#2219L], [cast(id#2240 as bigint)], Inner, BuildRight, false
# MAGIC                                           :- HashAggregate(keys=[srch_ci#2212, srch_co#2213, dtMonth#2171, hotel_id#2219L], functions=[partial_count(hotel_id#2219L) AS count#2255L], output=[srch_ci#2212, srch_co#2213, dtMonth#2171, hotel_id#2219L, count#2255L])
# MAGIC                                           :  +- Project [srch_ci#2212, srch_co#2213, hotel_id#2219L, add_months(cast(srch_ci#2212 as date), pos#2249) AS dtMonth#2171]
# MAGIC                                           :     +- Generate posexplode(repeat#2170), [srch_ci#2212, srch_co#2213, hotel_id#2219L], false, [pos#2249, col#2250]
# MAGIC                                           :        +- Project [srch_ci#2212, srch_co#2213, hotel_id#2219L, split(repeat(,, cast(round(months_between(cast(srch_co#2213 as timestamp), cast(srch_ci#2212 as timestamp), true, Some(Etc/UTC)), 0) as int)), ,, -1) AS repeat#2170]
# MAGIC                                           :           +- Filter (((size(split(repeat(,, cast(round(months_between(cast(srch_co#2213 as timestamp), cast(srch_ci#2212 as timestamp), true, Some(Etc/UTC)), 0) as int)), ,, -1), true) > 0) AND isnotnull(split(repeat(,, cast(round(months_between(cast(srch_co#2213 as timestamp), cast(srch_ci#2212 as timestamp), true, Some(Etc/UTC)), 0) as int)), ,, -1))) AND isnotnull(hotel_id#2219L))
# MAGIC                                           :              +- FileScan parquet db_sparksql.expedia_processed[srch_ci#2212,srch_co#2213,hotel_id#2219L] Batched: true, DataFilters: [(size(split(repeat(,, cast(round(months_between(cast(srch_co#2213 as timestamp), cast(srch_ci#22..., Format: Parquet, Location: PreparedDeltaFileIndex[dbfs:/db/expedia/processed], PartitionFilters: [], PushedFilters: [IsNotNull(hotel_id)], ReadSchema: struct<srch_ci:string,srch_co:string,hotel_id:bigint>
# MAGIC                                           +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[1, string, false] as bigint)),false), [id=#3918]
# MAGIC                                              +- Filter isnotnull(id#2240)
# MAGIC                                                 +- FileScan parquet db_sparksql.hotel_processed[address#2234,id#2240] Batched: true, DataFilters: [isnotnull(id#2240)], Format: Parquet, Location: PreparedDeltaFileIndex[dbfs:/db/hotel-weather/processed], PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<address:string,id:string>
# MAGIC 
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN COST
# MAGIC with 
# MAGIC expedia_processed_transformed as
# MAGIC (
# MAGIC   select c.srch_ci, c.srch_co, c.diffMonths, c.hotel_id, add_months(c.srch_ci, c.pos) as dtMonth
# MAGIC   from
# MAGIC   (
# MAGIC     select *, posexplode(a.repeat)
# MAGIC     from
# MAGIC     (
# MAGIC       select d.srch_ci, d.srch_co, d.diffMonths, d.hotel_id, split(repeat(',', diffMonths), ',') as repeat
# MAGIC       from
# MAGIC       (
# MAGIC         select a.srch_ci, a.srch_co, round(months_between(a.srch_co, a.srch_ci), 0) as diffMonths, a.hotel_id
# MAGIC         from expedia_processed a
# MAGIC       ) d
# MAGIC     ) a
# MAGIC   ) c
# MAGIC ),
# MAGIC expedia_transformed_count as
# MAGIC (
# MAGIC   select year(d.dtMonth) as year, month(d.dtMonth) as month, d.hotel_id, d.address, d.visits
# MAGIC     from (
# MAGIC       select a.srch_ci, a.srch_co, a.dtMonth, a.hotel_id, b.address, count(a.hotel_id) as visits
# MAGIC       from expedia_processed_transformed a
# MAGIC       inner join hotel_processed b on a.hotel_id=b.id
# MAGIC       group by a.srch_ci, a.srch_co, a.dtMonth, a.hotel_id, b.address
# MAGIC     ) d
# MAGIC   group by year, month, d.hotel_id, d.address, visits
# MAGIC )
# MAGIC select year, month, hotel_id, address, visits from
# MAGIC (select expedia_transformed_count.*,
# MAGIC row_number() over (partition by year, month order by visits desc) i
# MAGIC from expedia_transformed_count) t where i <= 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3

# COMMAND ----------

sqlDF3.explain()

# COMMAND ----------

sqlDF3.explain(extended=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN CODEGEN
# MAGIC select da.address, da.avg_tmpr_c, da.wthr_date, da.srch_ci, da.srch_co, da.avg_tmpr_c_co, db.avg_tmpr_c_ci, round((abs(da.avg_tmpr_c_co-db.avg_tmpr_c_ci)), 5) as weather_trend, round(((da.avg_tmpr_c_co+db.avg_tmpr_c_ci)/2), 5) as avg_tmpr_c_visits
# MAGIC from
# MAGIC (
# MAGIC   select d.address, d.avg_tmpr_c, d.wthr_date, d.srch_ci, d.srch_co,
# MAGIC   CASE WHEN d.wthr_date=d.srch_co THEN d.avg_tmpr_c ELSE NULL END as avg_tmpr_c_co
# MAGIC   from
# MAGIC   (
# MAGIC     select h.*
# MAGIC     from (
# MAGIC       select *
# MAGIC       from hotel_processed
# MAGIC       inner join expedia_processed
# MAGIC       on expedia_processed.hotel_id=hotel_processed.id
# MAGIC       where abs(day(srch_co)-day(srch_ci))>=7
# MAGIC     ) h
# MAGIC     where h.wthr_date=h.srch_co
# MAGIC   ) d
# MAGIC ) da
# MAGIC inner join
# MAGIC (
# MAGIC   select d.address, d.avg_tmpr_c, d.wthr_date, d.srch_ci, d.srch_co,
# MAGIC   CASE WHEN d.wthr_date=d.srch_ci THEN d.avg_tmpr_c ELSE NULL END as avg_tmpr_c_ci
# MAGIC   from
# MAGIC   (
# MAGIC     select h.*
# MAGIC     from (
# MAGIC       select *
# MAGIC       from hotel_processed
# MAGIC       inner join expedia_processed
# MAGIC       on expedia_processed.hotel_id=hotel_processed.id
# MAGIC       where abs(day(srch_co)-day(srch_ci))>=7
# MAGIC     ) h
# MAGIC     where h.wthr_date=h.srch_ci
# MAGIC   ) d
# MAGIC ) db
# MAGIC on (da.srch_ci = db.srch_ci and da.address = db.address)

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC == Optimized Logical Plan ==
# MAGIC Project [address#2645, avg_tmpr_c#2646, wthr_date#2655, srch_ci#2671, srch_co#2672, avg_tmpr_c_co#2602, avg_tmpr_c_ci#2603, round(abs((avg_tmpr_c_co#2602 - avg_tmpr_c_ci#2603), false), 5) AS weather_trend#2604, round(((avg_tmpr_c_co#2602 + avg_tmpr_c_ci#2603) / 2.0), 5) AS avg_tmpr_c_visits#2605], Statistics(sizeInBytes=497.7 EiB)
# MAGIC +- Join Inner, ((srch_ci#2671 = srch_ci#2705) AND (address#2645 = address#2679)), Statistics(sizeInBytes=591.0 EiB)
# MAGIC    :- Project [address#2645, avg_tmpr_c#2646, wthr_date#2655, srch_ci#2671, srch_co#2672, CASE WHEN (wthr_date#2655 = srch_co#2672) THEN avg_tmpr_c#2646 END AS avg_tmpr_c_co#2602], Statistics(sizeInBytes=205.1 GiB)
# MAGIC    :  +- Join Inner, ((wthr_date#2655 = srch_co#2672) AND (hotel_id#2678L = cast(id#2651 as bigint))), Statistics(sizeInBytes=244.5 GiB)
# MAGIC    :     :- Project [address#2645, avg_tmpr_c#2646, id#2651, wthr_date#2655], Statistics(sizeInBytes=266.0 KiB)
# MAGIC    :     :  +- Filter ((isnotnull(wthr_date#2655) AND isnotnull(id#2651)) AND isnotnull(address#2645)), Statistics(sizeInBytes=672.1 KiB)
# MAGIC    :     :     +- Relation[address#2645,avg_tmpr_c#2646,avg_tmpr_f#2647,city#2648,country#2649,geoHash#2650,id#2651,latitude#2652,longitude#2653,name#2654,wthr_date#2655,year#2656,month#2657,day#2658] parquet, Statistics(sizeInBytes=672.1 KiB)
# MAGIC    :     +- Project [srch_ci#2671, srch_co#2672, hotel_id#2678L], Statistics(sizeInBytes=31.5 MiB)
# MAGIC    :        +- Filter (((isnotnull(srch_co#2672) AND isnotnull(srch_ci#2671)) AND (abs((day(cast(srch_co#2672 as date)) - day(cast(srch_ci#2671 as date))), false) >= 7)) AND isnotnull(hotel_id#2678L)), Statistics(sizeInBytes=83.4 MiB)
# MAGIC    :           +- Relation[id#2659L,date_time#2660,site_name#2661,posa_continent#2662,user_location_country#2663,user_location_region#2664,user_location_city#2665,orig_destination_distance#2666,user_id#2667,is_mobile#2668,is_package#2669,channel#2670,srch_ci#2671,srch_co#2672,srch_adults_cnt#2673,srch_children_cnt#2674,srch_rm_cnt#2675,srch_destination_id#2676,srch_destination_type_id#2677,hotel_id#2678L] parquet, Statistics(sizeInBytes=83.4 MiB)
# MAGIC    +- Project [address#2679, srch_ci#2705, CASE WHEN (wthr_date#2689 = srch_ci#2705) THEN avg_tmpr_c#2680 END AS avg_tmpr_c_ci#2603], Statistics(sizeInBytes=110.4 GiB)
# MAGIC       +- Join Inner, ((wthr_date#2689 = srch_ci#2705) AND (hotel_id#2712L = cast(id#2685 as bigint))), Statistics(sizeInBytes=205.1 GiB)
# MAGIC          :- Project [address#2679, avg_tmpr_c#2680, id#2685, wthr_date#2689], Statistics(sizeInBytes=266.0 KiB)
# MAGIC          :  +- Filter ((isnotnull(wthr_date#2689) AND isnotnull(id#2685)) AND isnotnull(address#2679)), Statistics(sizeInBytes=672.1 KiB)
# MAGIC          :     +- Relation[address#2679,avg_tmpr_c#2680,avg_tmpr_f#2681,city#2682,country#2683,geoHash#2684,id#2685,latitude#2686,longitude#2687,name#2688,wthr_date#2689,year#2690,month#2691,day#2692] parquet, Statistics(sizeInBytes=672.1 KiB)
# MAGIC          +- Project [srch_ci#2705, hotel_id#2712L], Statistics(sizeInBytes=20.3 MiB)
# MAGIC             +- Filter (((isnotnull(srch_co#2706) AND isnotnull(srch_ci#2705)) AND (abs((day(cast(srch_co#2706 as date)) - day(cast(srch_ci#2705 as date))), false) >= 7)) AND isnotnull(hotel_id#2712L)), Statistics(sizeInBytes=83.4 MiB)
# MAGIC                +- Relation[id#2693L,date_time#2694,site_name#2695,posa_continent#2696,user_location_country#2697,user_location_region#2698,user_location_city#2699,orig_destination_distance#2700,user_id#2701,is_mobile#2702,is_package#2703,channel#2704,srch_ci#2705,srch_co#2706,srch_adults_cnt#2707,srch_children_cnt#2708,srch_rm_cnt#2709,srch_destination_id#2710,srch_destination_type_id#2711,hotel_id#2712L] parquet, Statistics(sizeInBytes=83.4 MiB)
# MAGIC 
# MAGIC == Physical Plan ==
# MAGIC AdaptiveSparkPlan isFinalPlan=false
# MAGIC +- Project [address#2645, avg_tmpr_c#2646, wthr_date#2655, srch_ci#2671, srch_co#2672, avg_tmpr_c_co#2602, avg_tmpr_c_ci#2603, round(abs((avg_tmpr_c_co#2602 - avg_tmpr_c_ci#2603), false), 5) AS weather_trend#2604, round(((avg_tmpr_c_co#2602 + avg_tmpr_c_ci#2603) / 2.0), 5) AS avg_tmpr_c_visits#2605]
# MAGIC    +- SortMergeJoin [srch_ci#2671, address#2645], [srch_ci#2705, address#2679], Inner
# MAGIC       :- Sort [srch_ci#2671 ASC NULLS FIRST, address#2645 ASC NULLS FIRST], false, 0
# MAGIC       :  +- Exchange hashpartitioning(srch_ci#2671, address#2645, 200), ENSURE_REQUIREMENTS, [id=#4850]
# MAGIC       :     +- Project [address#2645, avg_tmpr_c#2646, wthr_date#2655, srch_ci#2671, srch_co#2672, CASE WHEN (wthr_date#2655 = srch_co#2672) THEN avg_tmpr_c#2646 END AS avg_tmpr_c_co#2602]
# MAGIC       :        +- BroadcastHashJoin [wthr_date#2655, cast(id#2651 as bigint)], [srch_co#2672, hotel_id#2678L], Inner, BuildLeft, false
# MAGIC       :           :- BroadcastExchange HashedRelationBroadcastMode(ArrayBuffer(input[3, string, false], cast(input[2, string, false] as bigint)),false), [id=#4842]
# MAGIC       :           :  +- Filter ((isnotnull(wthr_date#2655) AND isnotnull(id#2651)) AND isnotnull(address#2645))
# MAGIC       :           :     +- FileScan parquet db_sparksql.hotel_processed[address#2645,avg_tmpr_c#2646,id#2651,wthr_date#2655] Batched: true, DataFilters: [isnotnull(wthr_date#2655), isnotnull(id#2651), isnotnull(address#2645)], Format: Parquet, Location: PreparedDeltaFileIndex[dbfs:/db/hotel-weather/processed], PartitionFilters: [], PushedFilters: [IsNotNull(wthr_date), IsNotNull(id), IsNotNull(address)], ReadSchema: struct<address:string,avg_tmpr_c:double,id:string,wthr_date:string>
# MAGIC       :           +- Filter (((isnotnull(srch_co#2672) AND isnotnull(srch_ci#2671)) AND (abs((day(cast(srch_co#2672 as date)) - day(cast(srch_ci#2671 as date))), false) >= 7)) AND isnotnull(hotel_id#2678L))
# MAGIC       :              +- FileScan parquet db_sparksql.expedia_processed[srch_ci#2671,srch_co#2672,hotel_id#2678L] Batched: true, DataFilters: [isnotnull(srch_co#2672), isnotnull(srch_ci#2671), (abs((day(cast(srch_co#2672 as date)) - day(ca..., Format: Parquet, Location: PreparedDeltaFileIndex[dbfs:/db/expedia/processed], PartitionFilters: [], PushedFilters: [IsNotNull(srch_co), IsNotNull(srch_ci), IsNotNull(hotel_id)], ReadSchema: struct<srch_ci:string,srch_co:string,hotel_id:bigint>
# MAGIC       +- Sort [srch_ci#2705 ASC NULLS FIRST, address#2679 ASC NULLS FIRST], false, 0
# MAGIC          +- Exchange hashpartitioning(srch_ci#2705, address#2679, 200), ENSURE_REQUIREMENTS, [id=#4851]
# MAGIC             +- Project [address#2679, srch_ci#2705, CASE WHEN (wthr_date#2689 = srch_ci#2705) THEN avg_tmpr_c#2680 END AS avg_tmpr_c_ci#2603]
# MAGIC                +- BroadcastHashJoin [wthr_date#2689, cast(id#2685 as bigint)], [srch_ci#2705, hotel_id#2712L], Inner, BuildLeft, false
# MAGIC                   :- BroadcastExchange HashedRelationBroadcastMode(ArrayBuffer(input[3, string, false], cast(input[2, string, false] as bigint)),false), [id=#4845]
# MAGIC                   :  +- Filter ((isnotnull(wthr_date#2689) AND isnotnull(id#2685)) AND isnotnull(address#2679))
# MAGIC                   :     +- FileScan parquet db_sparksql.hotel_processed[address#2679,avg_tmpr_c#2680,id#2685,wthr_date#2689] Batched: true, DataFilters: [isnotnull(wthr_date#2689), isnotnull(id#2685), isnotnull(address#2679)], Format: Parquet, Location: PreparedDeltaFileIndex[dbfs:/db/hotel-weather/processed], PartitionFilters: [], PushedFilters: [IsNotNull(wthr_date), IsNotNull(id), IsNotNull(address)], ReadSchema: struct<address:string,avg_tmpr_c:double,id:string,wthr_date:string>
# MAGIC                   +- Project [srch_ci#2705, hotel_id#2712L]
# MAGIC                      +- Filter (((isnotnull(srch_co#2706) AND isnotnull(srch_ci#2705)) AND (abs((day(cast(srch_co#2706 as date)) - day(cast(srch_ci#2705 as date))), false) >= 7)) AND isnotnull(hotel_id#2712L))
# MAGIC                         +- FileScan parquet db_sparksql.expedia_processed[srch_ci#2705,srch_co#2706,hotel_id#2712L] Batched: true, DataFilters: [isnotnull(srch_co#2706), isnotnull(srch_ci#2705), (abs((day(cast(srch_co#2706 as date)) - day(ca..., Format: Parquet, Location: PreparedDeltaFileIndex[dbfs:/db/expedia/processed], PartitionFilters: [], PushedFilters: [IsNotNull(srch_co), IsNotNull(srch_ci), IsNotNull(hotel_id)], ReadSchema: struct<srch_ci:string,srch_co:string,hotel_id:bigint>
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN COST
# MAGIC select da.address, da.avg_tmpr_c, da.wthr_date, da.srch_ci, da.srch_co, da.avg_tmpr_c_co, db.avg_tmpr_c_ci, round((abs(da.avg_tmpr_c_co-db.avg_tmpr_c_ci)), 5) as weather_trend, round(((da.avg_tmpr_c_co+db.avg_tmpr_c_ci)/2), 5) as avg_tmpr_c_visits
# MAGIC from
# MAGIC (
# MAGIC   select d.address, d.avg_tmpr_c, d.wthr_date, d.srch_ci, d.srch_co,
# MAGIC   CASE WHEN d.wthr_date=d.srch_co THEN d.avg_tmpr_c ELSE NULL END as avg_tmpr_c_co
# MAGIC   from
# MAGIC   (
# MAGIC     select h.*
# MAGIC     from (
# MAGIC       select *
# MAGIC       from hotel_processed
# MAGIC       inner join expedia_processed
# MAGIC       on expedia_processed.hotel_id=hotel_processed.id
# MAGIC       where abs(day(srch_co)-day(srch_ci))>=7
# MAGIC     ) h
# MAGIC     where h.wthr_date=h.srch_co
# MAGIC   ) d
# MAGIC ) da
# MAGIC inner join
# MAGIC (
# MAGIC   select d.address, d.avg_tmpr_c, d.wthr_date, d.srch_ci, d.srch_co,
# MAGIC   CASE WHEN d.wthr_date=d.srch_ci THEN d.avg_tmpr_c ELSE NULL END as avg_tmpr_c_ci
# MAGIC   from
# MAGIC   (
# MAGIC     select h.*
# MAGIC     from (
# MAGIC       select *
# MAGIC       from hotel_processed
# MAGIC       inner join expedia_processed
# MAGIC       on expedia_processed.hotel_id=hotel_processed.id
# MAGIC       where abs(day(srch_co)-day(srch_ci))>=7
# MAGIC     ) h
# MAGIC     where h.wthr_date=h.srch_ci
# MAGIC   ) d
# MAGIC ) db
# MAGIC on (da.srch_ci = db.srch_ci and da.address = db.address)

# COMMAND ----------


