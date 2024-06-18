from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split



spark = SparkSession.builder \
        .appName("NetworkWordCount") \
        .config("spark.driver.port","4040") \
        .config("spark.driver.host", "127.0.0.1") \
        .getOrCreate()

lines = spark.readStream \
        .format("socket")\
        .option("host", "127.0.0.1") \
        .option("port", 9999) \
        .load()

# Split the lines into words
words = lines.select(explode(split(lines.value, " ")).alias("word"))

# Generate running word count
wordCounts = words.groupBy("word").count()

# Start running the query that prints the running counts to the console
query = wordCounts.writeStream.outputMode("complete").format("console").start()

# Wait for the termination of the query
query.awaitTermination()