from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, IntegerType

spark = SparkSession.builder.appName('p3').getOrCreate()

schema = StructType([StructField('Region',StringType(),True),
                     StructField("Product",StringType(),True),
                     StructField("Sales",IntegerType(),True),
                     StructField("Profit",IntegerType(),True),
                     StructField("Cost",IntegerType(),True)])
df = spark.read.csv(path="D:\Sales.csv",header= True, schema = schema)

df.show()

df.printSchema()