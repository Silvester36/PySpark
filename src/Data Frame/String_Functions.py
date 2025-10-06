from  pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructType,StructField,IntegerType
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('Df1').getOrCreate()

column = StructType([StructField('age',IntegerType(),True),
                     StructField('city',StringType(),True),
                     StructField("country",StringType(),True),
                     StructField("email",StringType(),True),
                     StructField('id',IntegerType(),True),
                     StructField("name",StringType(),True)])

df= spark.read.json(path="D:\Downloads\sample_data.json",multiLine=True,schema=column)

d1 = df.withColumn("upper_case",upper(col("Name")))
df.withColumn("Lower_case",lower(col("Name")))
df.withColumn("trim",trim(col("Name")))
df.withColumn("Symbol",rpad(col("Name"),18,"-"))

df.agg(collect_set(col("city"))).show(truncate = False)




