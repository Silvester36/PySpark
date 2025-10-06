
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Df2').getOrCreate()

column = StructType([StructField('age',IntegerType(),True),
                     StructField('city',StringType(),True),
                     StructField("country",StringType(),True),
                     StructField("email",StringType(),True),
                     StructField('id',IntegerType(),True),
                     StructField("name",StringType(),True)])


w = Window.partitionBy(col("age")).orderBy(col('id'))
w2 = Window.orderBy("id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df= spark.read.json(path="D:\Downloads\sample_data.json",multiLine=True,schema=column)

df.withColumn('row_number',row_number().over(w)).show()
df.withColumn('rank',rank().over(w)).show()
df.withColumn('dense_rank',dense_rank().over(w)).show()
df.withColumn('n_tile',ntile(2).over(w)).show(n=50)
df.withColumn('cu_me_dist',cume_dist().over(w)).show()
df.withColumn("per_rank",percent_rank().over(w)).show(n=50)
df.withColumn('Lag',lag('email',1).over(w)).show(n=50)
df.withColumn('Lead',lead('email',1).over(w)).show(n=50)
df.withColumn("last_email", last(col("email")).over(w)).show(n=50)
df.withColumn('first',first('email').over(w)).show(n=50)