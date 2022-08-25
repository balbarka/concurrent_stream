# Databricks notebook source
# MAGIC %sh
# MAGIC head -n5 /Workspace/Repos/brad.barker@databricks.com/concurrent_stream/databricks/streaming/stream_A_class.py

# COMMAND ----------

import imp
mdl_a = imp.load_source('stream_A','/Workspace/Repos/brad.barker@databricks.com/concurrent_stream/databricks/streaming/stream_A_class.py')
mdl_b = imp.load_source('stream_B','/Workspace/Repos/brad.barker@databricks.com/concurrent_stream/databricks/streaming/stream_B_class.py')

# COMMAND ----------

stream_a = mdl_a.Stream_a()

# COMMAND ----------

stream_A = stream_A.Stream_A()

# COMMAND ----------

?imp.load_source

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -la /Workspace/Repos/brad.barker@databricks.com/concurrent_stream/databricks/streaming

# COMMAND ----------

stream_nb_names_classes = {'stream_A_class': 'Stream_A',
                           'stream_B_class': 'Stream_B'}

streams = []
for k, v in stream_nb_names_classes.items():
    nb_path = '/'.join(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[:-2] + [k,])
    dbutils.notebook.run(nb_path, timeout_seconds=0)

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/<user-name>/<repo-name>")

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -la /Workspace/Repos/brad.barker@databricks.com/concurrent_stream/databricks/job/multi_stream_thread

# COMMAND ----------

dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# COMMAND ----------

spark.conf.get('spark.databricks.clusterUsageTags.clusterId') == '0811-165815-c2envhsf'

# COMMAND ----------

import threading as t

class StreamNotebook:
  
  def __init__(self, stream_nb_name):
    self.nb_path = '/'.join(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[:-2] + [stream_nb_name,])
    
  def run(self):
    proc = dbutils.notebook.run(self.nb_path, timeout_seconds=0)
    print(proc.__class__)
  
stream_nb_names = ['stream_A_thread', 'stream_B_thread']
streamNotebooks = [StreamNotebook(j) for j in stream_nb_names]
streamNotebook_threads = [t.Thread(target=sn.run) for sn in streamNotebooks]

for t in streamNotebook_threads:
  t.start()

# COMMAND ----------

from os import environ

# COMMAND ----------

xxx = list(environ)

# COMMAND ----------

environ['SHLVL']

# COMMAND ----------

xxx

# COMMAND ----------

sn = streamNotebooks[0]

# COMMAND ----------

del sn

# COMMAND ----------

sn.

# COMMAND ----------

sn.__class__

# COMMAND ----------

sn.proc.__class__

# COMMAND ----------

t = stream_nb_threads[0]

# COMMAND ----------

t.join(10)

# COMMAND ----------



# COMMAND ----------

?dbutils.notebook.run

# COMMAND ----------

[t.is_alive() for t in stream_nb_threads]

# COMMAND ----------

for t in stream_nb_threads:
  t.is_alive()

# COMMAND ----------

spark.streams.active

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN concurrent;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) cnt FROM concurrent.tbl_a_thread

# COMMAND ----------

t.

# COMMAND ----------

spark.streams.active

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC 2 alternate considerations:
# MAGIC   * Instead of putting timeout in the individual stream notebooks, run all concurrently without waitinig for task completion, consider: `import threading as t`, Then use `spark.streams.active`
# MAGIC   * run the individual notebooks to get stream methods to start a writer, but don't execute until in master notebook context, then use `spark.streams.active`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Intent of this notebook is to provide an entry point to kick of multiple streaming notebooks. We'll then evaluate the target tables of those streaming notebooks and confirm that they infact did run concurrently.

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

def runStream(stream_nb_name):
  nb_path = '/'.join(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[:-2] + [stream_nb_name,])
  dbutils.notebook.run(nb_path, 80)
  
stream_nb_names = ['stream_A', 'stream_B']

with ThreadPoolExecutor() as executor:
    results = executor.map(runStream, stream_nb_names)

# COMMAND ----------



spark.streams.active


# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES in concurrent;

# COMMAND ----------

# MAGIC %md
# MAGIC    We can now evalaute the timestamps from these and confirm that they are running concurrently.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC    60 * 60 * HOUR(a.timestamp) + 60 * MINUTE(a.timestamp) + SECOND(a.timestamp) ts_a, 
# MAGIC    60 * 60 * HOUR(b.timestamp) + 60 * MINUTE(b.timestamp) + SECOND(b.timestamp) ts_b, 
# MAGIC    a.value     value
# MAGIC FROM
# MAGIC    concurrent.tbl_a a
# MAGIC INNER JOIN
# MAGIC    concurrent.tbl_b b
# MAGIC ON
# MAGIC     a.value = b.value
# MAGIC ORDER BY a.value
# MAGIC LIMIT 1000

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
# MAGIC    concurrent.tbl_a a
# MAGIC INNER JOIN
# MAGIC    concurrent.tbl_b b
# MAGIC ON
# MAGIC     a.value = b.value
# MAGIC ORDER BY a.value
# MAGIC LIMIT 1000

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can now clean-up our space:

# COMMAND ----------

dbutils.notebook.entry_point.getDbutils().notebook().run(path=xxx, timeout_seconds=600)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC     60 * 60 * HOUR(ct) + 60 * MINUTE(ct) + SECOND(ct)
# MAGIC FROM
# MAGIC     (SELECT CURRENT_TIMESTAMP() ct) a

# COMMAND ----------


