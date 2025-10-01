from  pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('Df1').getOrCreate()

df= spark.read.json(path="D:\Downloads\sample_data.json",multiLine=True)


df2 = df.withColumn(colName='age',col=col('age')+2)
df3 = df.withColumn(colName='age+2',col=col('age')+2)

df2.show()
df3.show()
