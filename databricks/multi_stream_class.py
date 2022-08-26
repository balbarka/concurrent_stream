# Databricks notebook source
# This is how we can import code from other files without having to build an application
# NOTE: an alternative is to build a whl file, install and import the respective classes
import imp
py_path = '/'.join(['', 'Workspace'] + dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[1:-1] + ['streaming', ''])
mdl_a = imp.load_source('stream_A', py_path + 'stream_A_class.py')
mdl_b = imp.load_source('stream_B', py_path + 'stream_B_class.py')

# COMMAND ----------

# This is a list of the above instantiated classes, it will include all the streams that we want running together in this job
streams = [mdl_a.Stream_a(),
           mdl_b.Stream_b()]

# COMMAND ----------

# This will initialize the demo state & then kick off the streamingQuery for each of our streaming classes
for s in streams:
  s.demo_initialize()
  s.start()

# COMMAND ----------

# Since all stream jobs are instantiated in the same spark streaming context, we can access the streamingQueries via spark context:
spark.streams.active

# COMMAND ----------

# We can also access the streamingQueries via the classes (note the memory space isn't the same, but they both point to the same set of streamingQueries)
# This will become a useful reference if we ever want to have methods from our classes act on the streamingQueries
[s.streamingQuery for s in streams]

# COMMAND ----------

# We can now manage the streams however, we like. In the example below, we'll run the queries for a predetermined amount of time before stopping all streams.
import time
time.sleep(180)
for s in spark.streams.active:
  s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC    We can now evalaute the timestamps from these and confirm that they are running concurrently.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC    60 * 60 * HOUR(a.timestamp) + 60 * MINUTE(a.timestamp) + SECOND(a.timestamp) ts_a, 
# MAGIC    60 * 60 * HOUR(b.timestamp) + 60 * MINUTE(b.timestamp) + SECOND(b.timestamp) ts_b
# MAGIC FROM
# MAGIC    concurrent.tbl_a_class a
# MAGIC INNER JOIN
# MAGIC    concurrent.tbl_b_class b
# MAGIC ON
# MAGIC     a.value = b.value
# MAGIC ORDER BY a.value
# MAGIC LIMIT 1000

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can now clean-up our space:

# COMMAND ----------

# Since we provided the clean up method in the class, we can call them here to clean up our demo:
for s in streams:
  s.demo_cleanup()
