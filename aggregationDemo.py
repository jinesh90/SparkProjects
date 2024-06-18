from pyspark.sql import *
from pyspark.sql.functions import  *


spark = SparkSession.builder \
    .master("local[3]") \
    .appName("AggDemo") \
    .getOrCreate()


invoice_df = spark.read \
            .format("csv")\
            .option("header","true") \
            .option("inferSchema","true") \
            .load("data/invoices.csv")

invoice_df.select(count("*").alias(("Total")),sum("Quantity").alias("TotalQuantity"),countDistinct("InvoiceNo").alias("CountDist"),round(avg("UnitPrice").alias("Avg"),2)).show()

# crate temp view
invoice_df.createOrReplaceTempView("sales")

summary_sql = spark.sql(
    """
    SELECT Country, InvoiceNo, sum(Quantity) as TotalQuantity,
    round(sum(Quantity * UnitPrice),2) as InvoiceValue FROM sales 
    GROUP BY Country, InvoiceNo
    """
)
summary_sql.show()

