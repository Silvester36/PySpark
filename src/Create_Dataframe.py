from pyspark.sql import SparkSession
spark =SparkSession.builder.appName('p1').getOrCreate()

value = [(1,'Silva'),(2,'Silvester'),(3,'Silvester Silva')]
column =['S.no','Name']

df =spark.createDataFrame(value,column)
df.show()