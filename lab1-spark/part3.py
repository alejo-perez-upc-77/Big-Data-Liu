from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
temperature_file = sc.textFile("C:\\Users\\marty\\Desktop\\temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = (year,temperature)
year_temperature = lines.map(lambda x: ((x[1][0:4], x[1][5:7],x[1][8:10], x[0]), (float(x[3]), float(x[3]))))

#filter
year_temperature = year_temperature.filter(lambda x: int(x[0][0])>=1960 and int(x[0][0])<=2014)


#Get max
max_temperature = year_temperature.reduceByKey(lambda a,b: (min(a[0],b[0]), max(a[1],b[1]))).map(lambda x: ((x[0][0],x[0][1],x[0][3]), (x[1][0],x[1][1], 1)))
max_temperature = max_temperature.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1],a[2]+b[2])).map(lambda x: (x[0],(x[1][0]+x[1][1],2*x[1][2]))).mapValues(lambda x: x[0]/x[1])
max_temperature = max_temperature.sortBy(ascending = False, keyfunc=lambda k: k[1])


# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder

max_temperature.saveAsTextFile("BDA/output")