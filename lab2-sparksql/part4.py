from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 1")

# This path is to the file on hdfs
temperature_file = sc.textFile("C:\\Users\\marty\\Desktop\\temperature-readings.csv")
precipitation_file = sc.textFile("C:\\Users\\marty\\Desktop\\precipitation-readings.csv")
temp = temperature_file.map(lambda line: line.split(";")).map(lambda x: Row(station = x[0], temp = float(x[3])))
prec = precipitation_file.map(lambda line: line.split(";")).map(lambda x: Row(station = x[0], prec = float(x[3])))

print("test1")
# dataframe
sqlContext = SQLContext(sc)
df_temp = sqlContext.createDataFrame(temp)
df_temp.registerTempTable("temp")

df_prec = sqlContext.createDataFrame(prec)
df_prec.registerTempTable("prec")
#filter

df_prec = df_prec.groupBy("station").agg(F.max("prec").alias("prec")).where((F.col("prec") >= 100) & (F.col("prec") <= 200))
df_temp = df_temp.groupBy("station").agg(F.max("temp").alias("temp")).where((F.col("temp") >= 25) & (F.col("temp") <= 30))

data_selected = df_temp.join(df_prec, ["station"]).orderBy('station', ascending=False)

# data_selected = data_selected.where((F.col("temp") >= 25) & (F.col("temp") <= 30) & (F.col("prec") >= 100) & (F.col("prec") <= 200))\
#     .orderBy('station', ascending=False)

print("test4")

#print(max_temperatures.collect())

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
data_selected.show()