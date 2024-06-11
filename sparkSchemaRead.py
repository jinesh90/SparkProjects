from pyspark.sql import SparkSession
from lib.logger import Log4J
from pyspark.sql.types import StructType,StructField,StringType,DateType,IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder \
            .master("local[3]") \
            .appName("SchemaApp") \
            .getOrCreate()

    logger = Log4J(spark)

    # define schema here
    flightSchemaStruct = StructType ([
        StructField("FL_DATE",DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType()),
        ]
    )

    flightTimeCsvDF = spark.read \
                        .format("csv") \
                        .option("header","true") \
                        .schema(flightSchemaStruct) \
                        .option("mode","FAILFAST") \
                        .option("dateFormat","M/d/y") \
                        .load("data/flight*.csv")

    flightTimeCsvDF.show(5)

    logger.info("CSV Schema: " + flightTimeCsvDF.schema.simpleString())

    # flightTimeJsonDF = spark.read \
    #                     .format("json") \
    #                     .load("data/flight*.json")
    #
    # flightTimeJsonDF.show(5)
    #
    # logger.info("JSON Schema: " + flightTimeJsonDF.schema.simpleString())
    #
    #
    # flightTimeParquetDF = spark.read \
    #                     .format("parquet") \
    #                     .load("data/flight*.parquet")
    #
    # flightTimeParquetDF.show(5)
    #
    # logger.info("Parquet Schema: " + flightTimeParquetDF.schema.simpleString())





