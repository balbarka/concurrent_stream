# Databricks notebook source
# MAGIC %md
# MAGIC This process will be used with a multi-task job, we will set the spark scheduler pool to be able to identify which stream is using which pool:

# COMMAND ----------

from pyspark.sql.functions import col, lit

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "poolA")

dbutils.fs.rm('dbfs:/tmp/checkpoint/stream_a_task', True)
stream_a = spark.readStream \
                .format("rate") \
                .option("rowsPerSecond", 10) \
                .load() \
                .withColumn('desc', lit('stream_A_task'))

write_a = stream_a.writeStream \
                  .queryName("stream_a") \
                  .option("checkpointLocation", "dbfs:/tmp/checkpoint/stream_a_task") \
                  .trigger(processingTime='10 seconds') \
                  .toTable("concurrent.tbl_a_task")

# COMMAND ----------


