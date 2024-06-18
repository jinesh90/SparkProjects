from pyspark.sql import *
from pyspark.sql.functions import  *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .master("local[3]") \
    .appName("RowTransformation") \
    .enableHiveSupport() \
    .getOrCreate()

def to_date_df(df, fmt, fld):
    return df.withColumn(fld, to_date(col(fld), fmt))


my_schema = StructType([
    StructField("ID", StringType()),
    StructField("EventDate", StringType())
])

my_rows = [Row("123","04/05/2004"),Row("234","12/05/2002"),Row("345","12/12/2009"),Row("456","01/07/1999")]
my_rdd = spark.sparkContext.parallelize(my_rows,2)
my_df = spark.createDataFrame(my_rdd,my_schema)
my_df.printSchema()
my_df.show()
new_df = to_date_df(my_df,"M/d/y","EventDate")
new_df.printSchema()
new_df.show()

