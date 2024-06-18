from pyspark.sql import *
from pyspark.sql.functions import  *
from pyspark.sql.types import IntegerType,StringType

def convert_country(country):
    if country == "USA":
        return "United States"
    return country


spark = SparkSession.builder \
    .master("local[3]") \
    .appName("UDFDemo") \
    .enableHiveSupport() \
    .getOrCreate()


simple_df = spark.read \
            .format("csv")\
            .option("header","true") \
            .option("inferSchema","true") \
            .load("data/simple.csv")

simple_df.show(10)


# this udf is just in memory not in catalog.
convert_country_udf = udf(convert_country,StringType())

# this will register in catalog.
spark.udf.register("convert_country_udf",convert_country,StringType())

simple_df_2 = simple_df.withColumn("Country",convert_country_udf(simple_df["Country"]))

simple_df_2.show(10)


# cast column in type.
simple_df_3 = simple_df.withColumn("Age",col("Age").cast(StringType()))

simple_df.printSchema()

simple_df_3.printSchema()

# drop duplicates (use unique combination)
simple_df_4 = simple_df.dropDuplicates(["Name","Age","Gender"])

simple_df_4.show(10)

