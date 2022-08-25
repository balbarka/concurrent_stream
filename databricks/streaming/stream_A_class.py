from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

class Stream_a:
  
  def __init__(self):
      self.spark = SparkSession.getActiveSession()
      self.checkPoint = 'dbfs:/tmp/checkpoint/stream_a_class'
      self.stream_df = self.spark.readStream \
                           .format("rate") \
                           .option("rowsPerSecond", 10) \
                           .load() \
                           .withColumn('desc', lit('stream_A_class'))  
    
  def start(self):
    self.streamingQuery = self.stream_df.writeStream \
                               .option("checkpointLocation", self.checkPoint) \
                               .trigger(processingTime='10 seconds') \
                               .toTable("concurrent.tbl_a_class")

    
  def demo_initialize(self):
    sql_cmd = ["CREATE DATABASE IF NOT EXISTS concurrent",
           """CREATE TABLE IF NOT EXISTS concurrent.tbl_a_class (
                 timestamp    TIMESTAMP, 
                 value        LONG, 
                 `desc`       STRING)""",
           "DELETE FROM concurrent.tbl_a_class WHERE 1=1"]
    for cmd in sql_cmd:
        self.spark.sql(cmd)
    
  def demo_cleanup(self):
    self.spark.sql("DROP TABLE IF EXISTS concurrent.tbl_a_class")
