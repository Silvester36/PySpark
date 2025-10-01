from  pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType,StructType,StructField,IntegerType

spark = SparkSession.builder.appName('Df1').getOrCreate()

column = StructType([StructField('age',IntegerType(),True),
                     StructField('city',StringType(),True),
                     StructField("country",StringType(),True),
                     StructField("email",StringType(),True),
                     StructField('id',IntegerType(),True),
                     StructField("name",StringType(),True)])

df= spark.read.json(path="D:\Downloads\sample_data.json",multiLine=True,schema=column)

df1 =df.where((col('id')>2) & (col('id')<10)).select('email','name','id')
df1.show()
df.filter(col('age')>=45).show()