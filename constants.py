#!/usr/bin/env/python

"""

[FP-LY-NY] Constants Module

Constants for the cli and utility modules.

"""
import os
from pyspark import StorageLevel

# FP-LY-NY Messaging
APP_NAME = "Find->People->Like->You->Near->You"
WELCOME_MSG = "[FP-LY-NY] Welcome to \'Find->People->Like->You->Near->You Application\'"
THANK_YOU_MSG="[FP-LY-NY] Thank You for using \'Find->People->Like->You->Near->You\' App.\n GoodBye!"

RESULT_BANNER="FP-LY-NY -> Top 10 People In Close Proximity"
LOC_RESULT_BANNER="FP-LY-NY -> Top 10 People In Close Proximity with Exact Location"

# FPLYNY Dummy Data File
FPLYNY_DATA_FILE = os.path.join(os.getcwd(), 'resources/dmclean.csv')


# FPLYNY Test Data File
FPLYNY_TEST_FILE = os.path.join(os.path.dirname(os.getcwd()),'resources/dmclean.csv')

CNT_DATA_FILE = os.path.join(os.getcwd(), 'resources/dmclean.csv')

# FPLYNY Utility Method File References

#removeNaNValues
NaN_IN_FILE = os.path.join(os.getcwd(), 'resources/dmclean.csv')
NaN_OUT_FILE = os.path.join(os.getcwd(), 'resources/dmfinal_no_dups.csv')

#appendLocationToCsv

APND_IN_FILE = os.path.join(os.getcwd(), 'resources/dmclean.csv')
APND_OUT_FILE = os.path.join(os.getcwd(), 'resources/dmfinal_loc.csv')

#mergeCSVs

MRG_IN_DIR = os.path.join(os.getcwd(), 'resources')
MRG_OUT_FILE = os.path.join(os.getcwd(), 'resources/dm_mrg.csv')


# Apache SPARK 1.6.2 | 2.0.2 Configuration Parameter Constants

# Spark Env Settings
PYSPARK162_HOME_DIR = "/home/aparasur/dev/spark-1.6.2-bin-hadoop2.6/python/pyspark"
PYSPARK202_HOME_DIR = "/home/aparasur/dev/spark-2.0.2-bin-hadoop2.7/python/pyspark"

SP162_HOME_DIR = '/home/aparasur/dev/spark-1.6.2-bin-hadoop2.6'
SP202_HOME_DIR = '/home/aparasur/dev/spark-2.0.2-bin-hadoop2.7'


# CSV Datasource - Databricks
# A library for parsing and querying CSV data with Apache Spark, for Spark SQL and DataFrames.

DATABRICKS_SPARK_CSV = 'com.databricks.spark.csv'


# Spark Tuning Parameters

#SP_SUBMIT_ARGS = '--verbose --packages com.databricks:spark-csv_2.10:1.3.0 --driver-java-options -XX:MaxPermSize=8192M'

SP_SUBMIT_ARGS = '--packages com.databricks:spark-csv_2.10:1.3.0  --driver-java-options -XX:MaxPermSize=8192M'

PACKAGES = '--packages com.databricks:spark-csv_2.10:1.3.0,' \
           'com.holdenkarau:spark-testing-base_2.10:1.5.2_0.3.3' # TESTING

MASTER_LOCAL_STANDALONE = "local"
MASTER_LOCAL_PARALLELIZE = "local[*]"
MASTER_LOCAL_10 = "local[10]"
MASTER_LOCAL_20 = "local[8]"
MASTER_LOCAL_50 = "local[8]"


# SP_DAEMON_MEM = "10g"
# SPARK_DRIVER_MEMORY = "10g"
# SPARK_EXECUTOR_MEMORY = "10g"
# SPARK_EXECUTOR_INSTANCES = "100"
# SPARK_EXECUTOR_CORES = "8"

SPARK_DYNAMIC_ALLOCATION_ENABLE = "false"
SPARK_SHUFFLE_SRVC_ENABLE = "false"
SPARK_SERIALIZER = "org.apache.spark.serializer.KryoSerializer"

#MEM_STORAGE_LEVEL = StorageLevel.MEMORY_ONLY_SER

# TRIALS
#------------------------------------------
# 1 - 5 queries
# [FP-LY-NY] Loaded data in memory in 10.268 seconds
# 1st query in 22.668 seconds
# Further queries b/w 8.371 - 9.322 seconds

MASTER = "local[*]"
SP_DAEMON_MEM = "10g"
SPARK_DRIVER_MEMORY = "10g"
SPARK_EXECUTOR_MEMORY = "10g"
SPARK_EXECUTOR_INSTANCES = "150"
SPARK_EXECUTOR_CORES = "10"
MEM_STORAGE_LEVEL = StorageLevel.MEMORY_ONLY

#------------------------------------------

# 2
# [FP-LY-NY] Loaded data in memory in 10.268 seconds
# 1st query in 22.668 seconds
# Further queries b/w 8.371 - 9.322 seconds

# MASTER = "local[10]"
# SP_DAEMON_MEM = "2g"
# SPARK_DRIVER_MEMORY = "2g"
# SPARK_EXECUTOR_MEMORY = "2g"
# SPARK_EXECUTOR_INSTANCES = "50"
# SPARK_EXECUTOR_CORES = "4"
# MEM_STORAGE_LEVEL = StorageLevel.MEMORY_ONLY_SER

#------------------------------------------








