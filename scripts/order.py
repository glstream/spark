from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustSpend")
sc = SparkContext(conf = conf)

def output(line):
    fields = line.split(',')
    custId= int(fields[0])
    itemId = int(fields[1])
    spend = float(fields[2])
    return (custId,itemId, spend)


lines = sc.textFile("/Users/graysonstream/Desktop/sparkCourse/data/customer-orders.csv")
parsedLines = lines.map(output)

custs = parsedLines.map(lambda x: (x[0], x[2]))
groupBy = custs.reduceByKey(lambda x, y: (x  + y))
remap = groupBy.map(lambda x: (x[1], x[0])).sortByKey(ascending= False)

results = remap.collect()

for result in results:
    cust = str(result[1])
    spend = result[0]
    print(cust, "%.2f" % spend)