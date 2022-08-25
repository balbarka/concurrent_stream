# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC This notebook is to demonstrate streaming into a table as part of a large demonstration of running concurrent streaming into table jobs.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS concurrent;
# MAGIC CREATE TABLE IF NOT EXISTS concurrent.tbl_b_timeout (
# MAGIC     timestamp    TIMESTAMP, 
# MAGIC     value        LONG, 
# MAGIC     `desc`       STRING);
# MAGIC DELETE FROM concurrent.tbl_b_timeout WHERE 1=1;

# COMMAND ----------

from pyspark.sql.functions import col, lit

dbutils.fs.rm('dbfs:/tmp/checkpoint/stream_b_timeout', True)
stream_b = spark.readStream \
                .format("rate") \
                .option("rowsPerSecond", 10) \
                .load() \
                .withColumn('desc', lit('stream_B_timeout'))

# COMMAND ----------

write_b = stream_b.writeStream \
                  .option("checkpointLocation", "dbfs:/tmp/checkpoint/stream_b_timeout") \
                  .trigger(processingTime='10 seconds') \
                  .toTable("concurrent.tbl_b_timeout")

# COMMAND ----------

write_b.awaitTermination(30)
write_b.stop()
