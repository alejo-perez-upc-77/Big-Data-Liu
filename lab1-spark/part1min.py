from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = (year,temperature)
year_temperature = lines.map(lambda x: (x[1][0:4], float(x[3])))

#filter
year_temperature = year_temperature.filter(lambda x: int(x[0])>=1950 and int(x[0])<=2014)

#Get min
min_temperatures = year_temperature.reduceByKey(lambda a,b:  min(a, b))

min_temperatures = min_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1])


# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
min_temperatures.saveAsTextFile("BDA/output")