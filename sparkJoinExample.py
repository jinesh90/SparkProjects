"""
Join Types

inner : intersection, takes common records only from left and right dataframe
outer: Full outer join, union, takes all records from both datafarmes ( leaft and right)
left: Left outer, takes all records from left dataframe.
right: Right outer, takes all records from right dataframe.

"""

from pyspark.sql import *
from lib.logger import Log4J
from pyspark.sql.functions import  *


spark = SparkSession.builder \
    .master("local[3]") \
    .appName("SparkJoin") \
    .getOrCreate()


logger = Log4J(spark)


order_list = [
    ("01","02",350,1),
    ("01", "04", 580, 1),
    ("01", "07", 320, 2),
    ("02", "03", 450, 1),
    ("02", "06", 220, 1),
    ("03", "01", 195, 1),
    ("04", "09", 270, 3),
    ("04", "08", 410, 2),
    ("05", "02", 350, 1),
]

order_df = spark.createDataFrame(order_list).toDF("order_id","prod_id","unit_price","qty")

product_list = [
    ("01","Scroll Mouse",250,20),
    ("02","Optical Mouse",350,20),
    ("03","Wireless Mouse",450,50),
    ("04","Wireless Keyboard",580,50),
    ("05","Standard Keyboard",360,10),
    ("06","16 GB Flash Storage",240,100),
    ("07","32 GB Flash Storage",302,50),
    ("08","64 GB Flash Storage",430,25),
]

product_df = spark.createDataFrame(product_list).toDF("prod_id","product_name","list_price","prod_qty")


order_df.show()
product_df.show()

# Inner join on prod_id column

# define join expression
join_expr = order_df.prod_id == product_df.prod_id

# define left inner join, this will return new df
join_df1 = order_df.join(product_df,join_expr,"inner") # left_df.join(right_df, join expression, type of join)

join_df1.select("order_id","product_name","unit_price","qty").show()


# Outer join on prod_id column
join_expr2 = order_df.prod_id == product_df.prod_id


# define left outer join, this will return new df
join_df2 = order_df.join(product_df,join_expr,"outer") # left_df.join(right_df, join expression, type of join)

join_df2.select("*").show()


# define left outer join, this will return new df
join_df3 = order_df.join(product_df,join_expr,"left") # left_df.join(right_df, join expression, type of join)

join_df3.select("*").show()


# define right outer join, this will return new df
join_df4 = order_df.join(product_df,join_expr,"right") # left_df.join(right_df, join expression, type of join)

join_df4.select("*").show()
