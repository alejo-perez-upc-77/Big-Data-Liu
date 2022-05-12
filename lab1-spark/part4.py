from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
prec_file = sc.textFile("BDA/input/precipitation-readings.csv")
lines_temp = temperature_file.map(lambda line: line.split(";"))
lines_prec = prec_file.map(lambda line: line.split(";"))

# (key, value) = (year,temperature)
year_temperature = lines_temp.map(lambda x: ( x[0] , float(x[3]))).reduceByKey(lambda a,b: max(a,b))
prec_measures = lines_prec.map(lambda x: (x[0], float(x[3]))).reduceByKey(lambda a,b: max(a,b))

final =  year_temperature.join(prec_measures)

final = final.filter(lambda x: float(x[1][0])>=25 and float(x[1][0])<=30).filter(lambda x: float(x[1][1])>= 100 and float(x[1][1])<= 200)

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
final.saveAsTextFile("BDA/output")