from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("totalAmountByCustomer")
sc = SparkContext(conf=conf)


def get_customer_purchase(line):
    cid = int(line.split(",")[0])
    price = float(line.split(",")[2])
    return (cid, price)


data = sc.textFile("./data/customer-orders.csv")
rdd = data.map(get_customer_purchase)

purchase = rdd.reduceByKey(lambda x,y:x+y).map(lambda x:(x[1],x[0])).sortByKey()

print(purchase.collect())

