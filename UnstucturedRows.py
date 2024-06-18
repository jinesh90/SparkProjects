from pyspark.sql import *
from pyspark.sql.functions import  *


spark = SparkSession.builder \
    .master("local[3]") \
    .appName("RowTransformation") \
    .enableHiveSupport() \
    .getOrCreate()

log_df = spark.read.text("data/linux.log")
log_df.printSchema()

log_regx = r"(\w{3} \d{1,2} \d{2}:\d{2}:\d{2}) (\w+) ([^:]+): (.*)"

log_df_parsed = log_df.select(
    regexp_extract('value', log_regx, 1).alias('timestamp'),
    regexp_extract('value', log_regx, 2).alias('hostname'),
    regexp_extract('value', log_regx, 3).alias('process'),
    regexp_extract('value', log_regx, 4).alias('message')
)

# check schema now
log_df_parsed.printSchema()

# transform string timestamp to date
log_df_parsed = log_df_parsed.withColumn('timestamp', to_timestamp('timestamp', 'MMM d HH:mm:ss'))

# check schema now
log_df_parsed.printSchema()

#apply column functions
log_df_parsed.groupBy("message") \
        .count() \
        .orderBy(desc('count')) \
        .show(50,truncate=False)
