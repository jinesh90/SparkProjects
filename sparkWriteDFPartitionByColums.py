from pyspark.sql import SparkSession
from lib.logger import Log4J

if __name__ == '__main__':
    spark = SparkSession.builder \
            .master("local[3]") \
            .appName("SchemaApp") \
            .getOrCreate()

    logger = Log4J(spark)

    flightTimeCsvDF = spark.read \
                        .format("csv") \
                        .option("header","true") \
                        .load("data/flight*.csv")

    logger.info("Num of Partitions before: " + str(flightTimeCsvDF.rdd.getNumPartitions()))

    flightTimeCsvDF.write \
        .format("json") \
        .mode("overwrite") \
        .option("path","dataOutput/json/") \
        .partitionBy("OP_CARRIER","ORIGIN") \
        .save()

    logger.info("Num of Partitions after: " + str(flightTimeCsvDF.rdd.getNumPartitions()))
