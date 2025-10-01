from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType

df = [("Silvester",[1,3,4,5]),("Roshan",[2,6,7,8])]
column = StructType([StructField("Name",StringType()),
                     StructField("Numbers",ArrayType(IntegerType(),True))])

spark = SparkSession.builder.appName("Df4").getOrCreate()
df = spark.createDataFrame(df,column)

df.show()
df.printSchema()