from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('RDD3').getOrCreate()

sc = spark.sparkContext

value = [(1,"Silva"),(2,'Silvester'),(3,'Silvester Silva')]
rdd = sc.parallelize(value)

df = spark.createDataFrame(rdd,['S.no','Name'])

df.show()