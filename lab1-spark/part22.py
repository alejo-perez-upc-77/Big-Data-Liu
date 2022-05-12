from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = (year,temperature)
year_temperature = lines.map(lambda x: ((x[1][0:4], x[1][5:7],x[0] ), float(x[3])))

#filter
year_temperature = year_temperature.filter(lambda x: int(x[0][0])>=1950 and int(x[0][0])<=2014)
year_temperature = year_temperature.filter(lambda x: x[1]>=10).map(lambda x: x[0]).distinct()

year_temperature = year_temperature.map(lambda x: ((x[0],x[1]),1))

#Get max
max_temperature = year_temperature.reduceByKey(lambda a,b: a+b)
max_temperature = max_temperature.sortBy(ascending = False, keyfunc=lambda k: k[0])

#print(max_temperatures.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
max_temperature.saveAsTextFile("BDA/output")