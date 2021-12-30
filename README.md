# spark-task

In this repo I read file 'data.csv' with two methods. In the first method i use SparkContext and it's textFile function to read data into rdd. In the second method i use
SparkSession and DataFrameReader to read csv file using a schema. 
Both methods calculate average delay time for every type of unit that use Advanced Life Support(ALSUnit). Then result show all units that have delay less 4 minutes. 

## For DataFrame:
![image](https://user-images.githubusercontent.com/32685300/147750704-1557e9f6-bd66-4b36-8121-0b79007874a5.png)
![image](https://user-images.githubusercontent.com/32685300/147752051-d79b819f-8e34-4212-8d53-9bd66bc5b300.png)


## For RDD:
![image](https://user-images.githubusercontent.com/32685300/147751742-06b7057c-4046-42a1-984d-1a01028dc9ae.png)
![image](https://user-images.githubusercontent.com/32685300/147752218-26bce2ec-27f7-4aa8-aad6-852078ca9d03.png)
