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
                        .load("data/flight*.csv")

    # flightTimeCsvDF.write \
    #     .format("csv") \
    #     .mode("overwrite") \
    #     .option("path","dataOutput/csv/") \
    #     .save()

    logger.info("Num of Partitions before: " + str(flightTimeCsvDF.rdd.getNumPartitions()))

    partitonedDF = flightTimeCsvDF.repartition(10)

    partitonedDF.write \
        .format("csv") \
        .mode("overwrite") \
        .option("path","dataOutput/csv/") \
        .save()

    logger.info("Num of Partitions after: " + str(partitonedDF.rdd.getNumPartitions()))
