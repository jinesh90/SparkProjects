import re
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("WordCountByBook")
sc = SparkContext(conf=conf)


def normalizedWords(text):
    return re.compile(r'\W+',re.UNICODE).split(text.lower())



inputfile = sc.textFile("./data/book.txt")


words = inputfile.flatMap(normalizedWords)


wordCounts = words.map(lambda x:(x,1)).reduceByKey(lambda x,y : x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)
print(wordCountsSorted.collect())



