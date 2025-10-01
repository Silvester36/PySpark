from  pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructType,StructField,IntegerType
from pyspark.sql.functions import col
spark = SparkSession.builder.appName('Df1').getOrCreate()

column = StructType([StructField('age',IntegerType(),True),
                     StructField('city',StringType(),True),
                     StructField("country",StringType(),True),
                     StructField("email",StringType(),True),
                     StructField('id',IntegerType(),True),
                     StructField("name",StringType(),True)])

df= spark.read.json(path="D:\Downloads\sample_data.json",multiLine=True,schema=column)

df1 = df.sort(df.age)
df1.show()
df2 = df.sort(col('id').desc())
df2.show()
