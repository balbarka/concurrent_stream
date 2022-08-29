# Databricks notebook source
# MAGIC %md
# MAGIC This process will be used with a multi-task job, we will set the spark scheduler pool to be able to identify which stream is using which pool:

# COMMAND ----------

from pyspark.sql.functions import col, lit

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "poolB")

dbutils.fs.rm('dbfs:/tmp/checkpoint/stream_b_task', True)
stream_a = spark.readStream \
                .format("rate") \
                .option("rowsPerSecond", 10) \
                .load() \
                .withColumn('desc', lit('stream_B_task'))

write_a = stream_a.writeStream \
                  .queryName("stream_b") \
                  .option("checkpointLocation", "dbfs:/tmp/checkpoint/stream_b_task") \
                  .trigger(processingTime='10 seconds') \
                  .toTable("concurrent.tbl_b_task")

# COMMAND ----------


