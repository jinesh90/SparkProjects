from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")

sc = SparkContext(conf=conf)

def parseLine(line):
    fields = line.split(",")
    stationId = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0/5.0) + 32.0
    return (stationId, entryType, temperature)

lines = sc.textFile("./data/1800.csv")

rdd = lines.map(parseLine)

# filter with TMIN values

minTemps = rdd.filter(lambda x: "TMAX" in x[1])

stationTemps = minTemps.map(lambda x: (x[0],x[2]))

minTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))

results = minTemps.collect()

for r in results:
    print(r[0] + "\t{:.2f}F".format(r[1]))

