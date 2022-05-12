from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 1")

# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# dataframe
data_temp = lines.map(lambda x: Row(year = int(x[1][0:4]), station = x[0], value = float(x[3])))

sqlContext = SQLContext(sc)

data = sqlContext.createDataFrame(data_temp)
data.registerTempTable("data_temp")

#filter
data_selected = data.filter((data["year"]>=1950) & (data["year"]<=2014)).groupBy('year').agg(F.first("station").alias("station"), F.min("value").alias("minvalue")).orderBy(['minvalue'], ascending = False)

#print(max_temperatures.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
data_selected.rdd.saveAsTextFile("BDA/output")