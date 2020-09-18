from pyspark.sql import SparkSession
from pyspark.sql import Row

import collections

spark = SparkSession.builder.config("spark.sql.warehouse.dir","file:///C:/temp/").appName("Spark SQL").getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), Name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFds = int(fields[3]))

lines = spark.sparkContext.textFile("D:\PySpark\Data")
people = lines.map(mapper)

schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <=19")


for teen in teenagers.collect():
    print(teen)

schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()
