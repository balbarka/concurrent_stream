# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Intent of this notebook is to provide an entry point to kick of multiple streaming notebooks. We'll then evaluate the target tables of those streaming notebooks and confirm that they infact did run concurrently.

# COMMAND ----------

nb_path = '/'.join(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[:-1] + ['streaming', 'stream_A_timeout'])
nb_path

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

def runStream(stream_nb_name):
  nb_path = '/'.join(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[:-1] + ['streaming', stream_nb_name])
  dbutils.notebook.run(nb_path, 80)
  
stream_nb_names = ['stream_A_timeout', 'stream_B_timeout']

with ThreadPoolExecutor() as executor:
    results = executor.map(runStream, stream_nb_names)

# COMMAND ----------

import time
time.sleep(30)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC If we look at the seconds since midnight for stream_a and stream_b ids, we can see that the streams were running concurrently:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC    60 * 60 * HOUR(a.timestamp) + 60 * MINUTE(a.timestamp) + SECOND(a.timestamp) ts_a, 
# MAGIC    60 * 60 * HOUR(b.timestamp) + 60 * MINUTE(b.timestamp) + SECOND(b.timestamp) ts_b, 
# MAGIC    a.value     value
# MAGIC FROM
# MAGIC    concurrent.tbl_a_timeout a
# MAGIC INNER JOIN
# MAGIC    concurrent.tbl_b_timeout b
# MAGIC ON
# MAGIC     a.value = b.value
# MAGIC ORDER BY a.value
# MAGIC LIMIT 1000

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can now clean-up our space:

# COMMAND ----------

import subprocess
spark.sql("DROP TABLE IF EXISTS concurrent.tbl_a_timeout")
subprocess.call("rm -rf /dbfs/tmp/checkpoint/stream_a_class", shell=True)
spark.sql("DROP TABLE IF EXISTS concurrent.tbl_a_class")
subprocess.call("rm -rf /dbfs/tmp/checkpoint/stream_a_class", shell=True)
