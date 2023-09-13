from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("WordCountByBook")
sc = SparkContext(conf=conf)


inputfile = sc.textFile("./data/book.txt")


words = inputfile.flatMap(lambda x: x.split())


wordCounts = words.countByValue()

for k,v in wordCounts.items():
    print("Word :{} Has Occurance: {}".format(k,v))

