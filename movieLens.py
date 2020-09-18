"""
from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("D:/PySPark/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
"""


from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("HistogramRating")
sc = SparkContext(conf = conf)

lines = sc.textFile("D:/PySpark/u.data")
ratings = lines.map(lambda x:x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key,value in sortedResults.items():
    print("%s %i" % (key,value))