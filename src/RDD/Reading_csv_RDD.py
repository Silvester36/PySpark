from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDD2").getOrCreate()
sc = spark.sparkContext
Read_csv = sc.textFile("D:\Sales.csv")
print(Read_csv.collect())