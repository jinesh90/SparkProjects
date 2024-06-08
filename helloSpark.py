from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_config

if __name__ == '__main__':


    conf = get_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4J(spark)

    logger.info("Starting Hello Spark")

    logger.info(spark.sparkContext.getConf().toDebugString())

    spark.stop()

    logger.info("Stopping Hello Spark")