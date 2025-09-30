from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('p3').getOrCreate()

df = spark.read.csv(path="D:\Sales.csv",header= True, inferSchema='True')

df.show()

df.printSchema()