from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, MapType
from pyspark.sql.functions import *

# Data
values = [
    ("Silvester", {'Skill1': 'Java', 'Skill2': 'Python'}),
    ("Silva", {'Skill1': 'C', 'Skill2': 'C++'})
]

# Schema
Data_Type = StructType([
    StructField('Name', StringType(), True),
    StructField('Skills', MapType(StringType(), StringType()))
])

# Spark session
spark = SparkSession.builder.appName('Df6').getOrCreate()

# DataFrame creation
df = spark.createDataFrame(values, schema=Data_Type)

# Extracting a specific key's value
df1 = df.withColumn("total", col("Skills")["Skill2"])

# Extracting map keys and values as arrays
df3 = df.withColumn("keys", map_keys(col("Skills")))
df4 = df.withColumn("values", map_values(col("Skills")))

# ✅ Correct exploding of MapType → gives two columns: key, value
df2 = df.select("*", explode(col("Skills")).alias("key", "value"))

# Display results
print("=== Exploded Map ===")
df2.show(truncate=False)

print("=== Map Values ===")
df4.show(truncate=False)

print("=== Map Keys ===")
df3.show(truncate=False)

print("=== Extract Skill2 ===")
df1.show(truncate=False)


df.printSchema()