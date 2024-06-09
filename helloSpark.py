from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_config,load_data_file,count_by_country
import sys

if __name__ == '__main__':


    conf = get_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    logger.info("Starting Hello Spark")

    logger.info(spark.sparkContext.getConf().toDebugString())


    simple_df = load_data_file(spark,sys.argv[1])

    partitoned_df = simple_df.repartition(2)

    count_df = count_by_country(partitoned_df)

    logger.info(count_df.collect())

    input("Press Enter")
    logger.info("Stopping Hello Spark")
    spark.stop()