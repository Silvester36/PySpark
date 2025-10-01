from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType
from pyspark.sql.functions import col, array

df = [("Silvester",'Python','Java'),("Roshan",'C++','C')]

column = StructType([StructField("Name",StringType()),
                     StructField("Skill1",StringType()),
                     StructField("Skill2",StringType())])

spark = SparkSession.builder.appName("Df6").getOrCreate()
df = spark.createDataFrame(df,column)

df2 = df.withColumn("Skills",array(col('Skill1'),col('Skill2')))

df2.show()