# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC This notebook is to demonstrate streaming into a table as part of a large demonstration of running concurrent streaming into table jobs.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS concurrent;
# MAGIC CREATE TABLE IF NOT EXISTS concurrent.tbl_a_timeout (
# MAGIC     timestamp    TIMESTAMP, 
# MAGIC     value        LONG, 
# MAGIC     `desc`       STRING);
# MAGIC DELETE FROM concurrent.tbl_a_timeout WHERE 1=1;

# COMMAND ----------

from pyspark.sql.functions import col, lit

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "poolA")

dbutils.fs.rm('dbfs:/tmp/checkpoint/stream_a_timeout', True)
stream_a = spark.readStream \
                .format("rate") \
                .option("rowsPerSecond", 10) \
                .load() \
                .withColumn('desc', lit('stream_A_timeout'))

# COMMAND ----------

write_a = stream_a.writeStream \
                  .queryName("stream_a") \
                  .option("checkpointLocation", "dbfs:/tmp/checkpoint/stream_a_timeout") \
                  .trigger(processingTime='10 seconds') \
                  .toTable("concurrent.tbl_a_timeout")

# COMMAND ----------

write_a.awaitTermination(30)
write_a.stop()

# COMMAND ----------


