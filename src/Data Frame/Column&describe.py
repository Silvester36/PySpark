from  pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Df1').getOrCreate()

df= spark.read.json(path="D:\Downloads\sample_data.json",multiLine=True)

print(df.columns)
df.describe().show()
