from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("JSON").getOrCreate()

print("Hello")