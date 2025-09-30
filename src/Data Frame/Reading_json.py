from pyspark.sql import SparkSession

Read = SparkSession.builder.appName('D1').getOrCreate()

df = Read.read.json(path="D:\Downloads\sample_data.json",multiLine=True)

df.show(truncate= False)
print(df.count())
