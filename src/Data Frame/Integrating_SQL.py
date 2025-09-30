from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('D1').getOrCreate()
df = spark.read.json(path="D:\Downloads\sample_data.json", multiLine=True)

df.createOrReplaceTempView("Sample")

result = spark.sql("""
    Select name,age
    from Sample
    where age > 30 AND age < 40
""")

result.show()
