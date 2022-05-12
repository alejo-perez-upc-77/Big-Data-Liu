from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F


sc = SparkContext(appName = "exercise 1")
sqlContext = SQLContext(sc)
stations = sc.textFile("C:\\Users\\marty\\Desktop\\stations-Ostergotland.csv").map(lambda x: x.split(";")[0]).collect()
precipitation_file = sc.textFile("C:\\Users\\marty\\Desktop\\precipitation-readings.csv")

prec = precipitation_file.map(lambda line: line.split(";"))

prec = prec.map(lambda x: Row(year = int(x[1][0:4]), month = int(x[1][5:7]), station = x[0], day = int(x[1][8:10]), prec = float(x[3])))

df_prec = sqlContext.createDataFrame(prec)
df_prec.registerTempTable("prec")
#filter



data_selected = df_prec.where((F.col("year")>=1993) & (F.col("year")<=2016) & (F.col("station").isin(stations))).groupBy('year', "month", "station")\
    .agg( F.sum("prec").alias("prec"))\
    .groupBy("year", "month").agg( F.avg("prec").alias("average"))\
    .sort("average", ascending = False) 

data_selected.rdd.saveAsTextFile("BDA/output")