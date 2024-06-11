from pyspark.sql import *
from lib.utils import get_config
import sys

if __name__ == '__main__':
    conf = get_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    sc = spark.sparkContext

    linesRDD = sc.textFile(sys.argv[1])

    partitionedRDD = linesRDD.repartition(2)

    colsRDD = partitionedRDD.map(lambda line:line.replace("USA","US"))

    data = colsRDD.collect()

    for x in data:
        print(x)




