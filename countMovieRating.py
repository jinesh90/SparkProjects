import collections
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("RatingHistogram")
sc = SparkContext(conf=conf)

lines = sc.textFile("./data/ml-100k/u.data")

ratings = lines.map(lambda x:x.split()[2])

result = ratings.countByValue()


sortedResults = collections.OrderedDict(sorted(result.items()))

for k,v in sortedResults.items():
    print("{} {}".format(k,v))

