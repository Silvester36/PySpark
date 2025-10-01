from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType
from pyspark.sql.functions import col, explode, split

df = [("Silvester",['Python,Java']),("Roshan",['C++,C'])]

column = StructType([StructField("Name",StringType()),
                     StructField("Skills",StringType())])

spark = SparkSession.builder.appName("Df6").getOrCreate()
df = spark.createDataFrame(df,column)

df2 = df.withColumn("Skills",split(col('Skills'),','))

df2.show()