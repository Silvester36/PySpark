from pyspark.sql import SparkSession
from pyspark.sql.types import *

schema = StructType([StructField("S.no",IntegerType(),True),
                     StructField("Name",StringType(),True)])

spark = SparkSession.builder.appName('p2').getOrCreate()

value = [(1,'Silvester'),(2,'Silva'),(3,'Silvester Silva')]

df = spark.createDataFrame(value,schema)

df.show()