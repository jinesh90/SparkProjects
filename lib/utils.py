import configparser

from pyspark import SparkConf


def get_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (k,v) in config.items("SPARK_APP_CONFIG"):
        spark_conf.set(k,v)
    return spark_conf

def load_data_file(spark, file):
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(file)
    return df

def count_by_country(df):
    count_df = df.where("Age > 30") \
        .select("Age", "Gender", "Name", "Country") \
        .groupBy("Country").count()
    return count_df