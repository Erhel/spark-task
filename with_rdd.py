from typing import List

from pyspark import RDD
from pyspark.sql import SparkSession


def split_line(line: str, sep: str) -> List[str]:
    words = []
    word = ""
    opened_quotes = False
    for i in range(len(line)):
        if line[i] == sep and opened_quotes is False:
            words.append(word)
            word = ""
        elif line[i] == "\"":
            opened_quotes = not opened_quotes
        elif i == len(line) - 1:
            word += line[i]
            words.append(word)
            word = ""
        else:
            word += line[i]
    return words


spark = SparkSession.builder.appName('rdd-task').getOrCreate()

raw_rdd: RDD = spark.sparkContext.textFile('data.csv', 10)

header = raw_rdd.first()
rdd = raw_rdd.filter(lambda line: line != header).map(lambda line: split_line(line, ','))

rdd = rdd.filter(lambda x: x[17] == 'true').map(lambda x: (x[20],  (float(x[-1]), 1))).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).map(lambda x: (x[0], x[1][0]/x[1][1])).sortBy(lambda x: x[1])

print(rdd.collect())

input()
