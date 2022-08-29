# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS concurrent;
# MAGIC CREATE TABLE IF NOT EXISTS concurrent.tbl_a_task (
# MAGIC     timestamp    TIMESTAMP, 
# MAGIC     value        LONG, 
# MAGIC     `desc`       STRING);
# MAGIC DELETE FROM concurrent.tbl_a_task WHERE 1=1;
# MAGIC CREATE TABLE IF NOT EXISTS concurrent.tbl_b_task (
# MAGIC     timestamp    TIMESTAMP, 
# MAGIC     value        LONG, 
# MAGIC     `desc`       STRING);
# MAGIC DELETE FROM concurrent.tbl_b_task WHERE 1=1;

# COMMAND ----------


