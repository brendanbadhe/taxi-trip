from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("csv_to_parquet").getOrCreate()
df1 = spark.read.csv(
    "datasets/yellow_tripdata_2015-01.csv", header=True, inferSchema=True
)
df2 = spark.read.csv(
    "datasets/yellow_tripdata_2016-01.csv", header=True, inferSchema=True
)
df3 = spark.read.csv(
    "datasets/yellow_tripdata_2016-02.csv", header=True, inferSchema=True
)
df4 = spark.read.csv(
    "datasets/yellow_tripdata_2016-03.csv", header=True, inferSchema=True
)
df1.write.parquet("datasets/yellow_tripdata_2015-01.parquet")
df2.write.parquet("datasets/yellow_tripdata_2016-01.parquet")
df3.write.parquet("datasets/yellow_tripdata_2016-02.parquet")
df4.write.parquet("datasets/yellow_tripdata_2016-03.parquet")
