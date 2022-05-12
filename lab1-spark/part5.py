from pyspark import SparkContext
import logging 

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs

stations = sc.textFile("BDA/input/stations-Ostergotland.csv")
prec_file = sc.textFile("BDA/input/precipitation-readings.csv")

stations = stations.map(lambda line: line.split(";")[0]).collect()
prec = prec_file.map(lambda line: line.split(";")).map(lambda x: (x[0], x[1][0:4], x[1][5:7], x[1][8:10], float(x[3]))).filter(lambda x: int(x[1]) >= 1993 and int(x[1]) <= 2016)
prec = prec.filter(lambda x: x[0] in stations)
prec = prec.map(lambda x: ((x[0], x[1], x[2]), x[4])).reduceByKey(lambda a,b: a + b).map(lambda x: ((x[0][1],x[0][2]), (x[1], 1)))

prec = prec.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1]))

prec = prec.mapValues(lambda x: (x[0]/x[1])).sortBy(ascending = False, keyfunc=lambda k: k[1])

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
prec.saveAsTextFile("BDA/output")
