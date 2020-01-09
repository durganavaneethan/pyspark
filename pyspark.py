from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession
spark = SparkSession.builder\
    .master("yarn")\
    .appName("Delta Detection") \
    .config("spark.dynamicAllocation.enabled","true") \
                .config("spark.debug.maxToStringFields","100") \
    .config("spark.dynamicAllocation.initialExecutors", "10") \
    .config("spark.dynamicAllocation.minExecutors", "10")\
    .config("spark.dynamicAllocation.maxExecutors", "50") \
    .getOrCreate()

todayDF = spark.read.format("csv").option("header", "true").option("delimiter", "^").load(Today_File_Path).limit(10)
yesterdayDF = spark.read.format("csv").option("header", "true").option("delimiter", "^").load(Yesterday_File_Path).limit(10)
