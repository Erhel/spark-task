from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.types import *

spark = SparkSession.builder.appName("dataframe-task").getOrCreate()

schema = StructType([
    StructField("CallNumber", IntegerType(), False),
    StructField("UnitID", StringType(), False),
    StructField("IncidentNumber", IntegerType(), False),
    StructField("CallType", StringType(), False),
    StructField("CallDate", StringType(), False),
    StructField("WatchDate", StringType(), False),
    StructField("CallFinalDisposition", StringType(), False),
    StructField("AvailableDtTm", StringType(), False),
    StructField("Address", StringType(), False),
    StructField("City", StringType(), False),
    StructField("Zipcode", IntegerType(), False),
    StructField("Battalion", StringType(), False),
    StructField("StationArea", IntegerType(), False),
    StructField("Box", IntegerType(), False),
    StructField("OriginalPriority", IntegerType(), False),
    StructField("Priority", IntegerType(), False),
    StructField("FinalPriority", IntegerType(), False),
    StructField("ALSUnit", BooleanType(), False),
    StructField("CallTypeGroup", StringType(), False),
    StructField("NumAlarms", IntegerType(), False),
    StructField("UnitType", StringType(), False),
    StructField("UnitSequenceInCallDispatch", IntegerType(), False),
    StructField("FirePreventionDistrict", IntegerType(), False),
    StructField("SupervisorDistrict", IntegerType(), False),
    StructField("Neighborhood", StringType(), False),
    StructField("Location", StringType(), False),
    StructField("RowID", StringType(), False),
    StructField("Delay", DoubleType(), False)
])

raw_dataframe = spark.read.csv("data.csv", header=True, schema=schema)

dataframe = raw_dataframe.filter(col("ALSUnit") == True).select("UnitType", "Delay").groupBy("UnitType").agg(avg("Delay").alias("AverageDelay")).filter(col("AverageDelay") < 4)

dataframe.show()
dataframe.explain()