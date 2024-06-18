from pyspark.sql import SparkSession
from lib.logger import Log4J

if __name__ == '__main__':
    spark = SparkSession.builder \
            .master("local[3]") \
            .appName("AirlineTableCreation") \
            .enableHiveSupport() \
            .getOrCreate()

    # create sql db

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")

    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    logger = Log4J(spark)

    flightTimeParquetDF = spark.read \
                        .format("parquet") \
                        .load("data/flight*.parquet")

    flightTimeParquetDF.write \
        .mode("overwrite") \
        .format("csv") \
        .bucketBy(5, "OP_CARRIER", "ORIGIN") \
        .saveAsTable("flight_data_table")



