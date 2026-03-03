from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestSpark") \
    .getOrCreate()

print("Spark is running!")

df = spark.createDataFrame([
    (1, "Bhageeratha", 23),
    (2, "SparkUser", 30)
], ["id", "name", "age"])

df.show()

spark.stop()
