from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

line = sc.textFile('/Users/graysonstream/Desktop/sparkCourse/data/fakefriends.csv')
rdd = line.map(parseLine)

totalsByAge = rdd.reduceByKey(lambda x, y: (x[0], y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = totalsByAge.collect()
for result in results:
    print(result)

# results = rdd.collect()
# for result in results:
#     print(result)



# totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
# averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
# results = averagesByAge.collect()
# for result in results:
#     print(result)