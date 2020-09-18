import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
import collections

spark = SparkSession.builder.config("spark.sql.warehouse.dir","file:///C:/temp/").appName("Top 10 movies").getOrCreate()

data = spark.sparkContext.textFile("D:/PySpark/Data/u.data")
res = data.map(lambda x: x.split('\t')[1])

kvp = res.map(lambda x:(x,1))
reducedVal = kvp.reduceByKey(lambda x,y: (x+y))
res1 = reducedVal.take(10)

for i in res1:
    print(i)
