from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, explode_outer, posexplode, posexplode_outer, col
spark = SparkSession.builder.appName("AllExplodeFunctions").getOrCreate()
data = [(1, ["Python", "Java"]),(2, None),(3, []),]

df = spark.createDataFrame(data, ["id", "skills"])

# 1️⃣ explode() - drops nulls/empty
df_explode = df.withColumn("skill_explode", explode(col("skills")))
print("explode():")
df_explode.show(truncate=False)

# 2️⃣ explode_outer() - keeps nulls/empty
df_explode_outer = df.withColumn("skill_explode_outer", explode_outer(col("skills")))
print("explode_outer():")
df_explode_outer.show(truncate=False)

# 3️⃣ pos_explode() - drops nulls/empty, gives position
df_pos_explode = df.select("id", posexplode(col("skills")).alias("pos", "skill"))
print("pos_explode():")
df_pos_explode.show(truncate=False)

# 4️⃣ pos_explode_outer() - keeps nulls/empty, gives position
df_pos_explode_outer = df.select("id", posexplode_outer(col("skills")).alias("pos", "skill"))
print("pos_explode_outer():")
df_pos_explode_outer.show(truncate=False)
