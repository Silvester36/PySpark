from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType
from pyspark.sql.functions import col, explode, split

df = [("Silvester",['Python,Java']),("Roshan",['C++,C'])]

column = StructType([StructField("Name",StringType()),
                     StructField("Numbers",StringType())])

spark = SparkSession.builder.appName("Df6").getOrCreate()
df = spark.createDataFrame(df,column)

df2 = df.withColumn("Numbers",split(col('Numbers'),','))

df2.show()