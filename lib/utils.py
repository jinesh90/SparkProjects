import configparser

from pyspark import SparkConf


def get_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (k,v) in config.items("SPARK_APP_CONFIG"):
        spark_conf.set(k,v)
    return spark_conf
