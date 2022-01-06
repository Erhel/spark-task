# spark-task

In this repo I read file 'data.csv' with two methods. In the first method i use SparkContext and it's textFile function to read data into rdd. In the second method i use
SparkSession and DataFrameReader to read csv file using a schema. 
Both methods calculate average delay time for every type of unit that use Advanced Life Support(ALSUnit). Then result show all units that have delay less 4 minutes. 

## For DataFrame:
![image](https://user-images.githubusercontent.com/32685300/147750704-1557e9f6-bd66-4b36-8121-0b79007874a5.png)

![image](https://user-images.githubusercontent.com/32685300/147752507-509d14a1-e5ee-43ab-a32a-7d3a4a842217.png)
![image](https://user-images.githubusercontent.com/32685300/148380223-50f419e4-3fc4-42e5-a65e-2e0b2310efad.png)
![image](https://user-images.githubusercontent.com/32685300/148380665-cc9dc7b0-e8c6-4c2d-960f-a4b85c1219dd.png)



## For RDD:
![image](https://user-images.githubusercontent.com/32685300/147751742-06b7057c-4046-42a1-984d-1a01028dc9ae.png)

![image](https://user-images.githubusercontent.com/32685300/147752594-c8aeb100-49cc-4b15-87d7-3af47aff31c7.png)
![image](https://user-images.githubusercontent.com/32685300/148379972-8f817628-11fe-4959-bd6c-86dc658ec7f0.png)
![image](https://user-images.githubusercontent.com/32685300/148380003-5769cd39-48bb-419b-a7e7-ad0f129ce5a4.png)

