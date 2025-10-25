from pyspark.sql import SparkSession

# Create a Spark session
spark = (
  SparkSession.builder
  .appName("hdfs-test")
  .getOrCreate()
)

# Write a range of 5 to HDFS
spark.range(5).write.mode("overwrite").csv("hdfs:///tmp/range5")

# Read the range of 5 from HDFS
print(spark.read.csv("hdfs:///tmp/range5").count())
