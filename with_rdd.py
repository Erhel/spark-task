from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("rdd-task").getOrCreate()

raw_rdd = spark.sparkContext.textFile("data.csv", 10)


header  = raw_rdd.first()
rdd = raw_rdd.filter(lambda line: line != header).map(lambda line: line.split(","))

result = rdd.filter(lambda x: x[17] == 'true').map(lambda x: (x[20],  (float(x[-1]), 1))).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).map(lambda x: (x[0], x[1][0]/x[1][1])).filter(lambda x: x[1] < 4).collect()

print(result)
