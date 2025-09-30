from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDD1").getOrCreate()
sc = spark.sparkContext
data =[1,2,3,4,5,6,6,7]
rdd = sc.parallelize(data)
print(rdd.collect())