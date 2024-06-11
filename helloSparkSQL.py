import sys
from pyspark.sql import *
from lib.logger import Log4J

if __name__ == '__main__':
    spark = SparkSession.builder \
            .master("local[3]") \
            .appName("HelloSparkSQL") \
            .getOrCreate()

    logger = Log4J(spark)


    simple_df = spark.read \
                .option("inferSchema","true") \
                .option("header","true") \
                .csv(sys.argv[1])

    simple_df.createOrReplaceTempView("simple_table")

    age_df = spark.sql("select * from simple_table where Age < 30")

    age_df.show()


