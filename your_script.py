from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create SparkSession manually (REQUIRED in spark-submit)
spark = SparkSession.builder \
    .appName("TestYarnSubmit") \
    .getOrCreate()

# Sample data
data = [
    Row(age=25, salary=50000),
    Row(age=30, salary=60000),
    Row(age=45, salary=80000),
    Row(age=50, salary=90000)
]

df = spark.createDataFrame(data)

df.show()

print("Total rows:", df.count())

spark.stop()
