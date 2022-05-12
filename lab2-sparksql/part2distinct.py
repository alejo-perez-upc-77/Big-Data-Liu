from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 1")

# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# dataframe
data_temp = lines.map(lambda x: Row(year = int(x[1][0:4]), month = int(x[1][5:7]), temp = float(x[3]), station = x[0]))
sqlContext = SQLContext(sc)
data = sqlContext.createDataFrame(data_temp)
data.registerTempTable("data")
#filter
print("test preselected")
data_selected = data.filter((data["year"]>=1950) & (data["year"]<=2014) & (data["temp"]> 10)).groupBy('year', "month") \
.agg( F.countDistinct("station").alias("count"))\
.orderBy(['count'], ascending = False)
print("test selected")

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
data_selected.rdd.saveAsTextFile("BDA/output")