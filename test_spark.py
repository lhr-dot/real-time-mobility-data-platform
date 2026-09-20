from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("mobility-test")
    .master("local[*]")
    .getOrCreate()
)

print(f"Spark version : {spark.version}")

data = [
    ("bus-1", 20),
    ("bus-2", 15),
    ("bus-3", 25),
]

df = spark.createDataFrame(data, ["vehicle_id", "speed_kmh"])

df.show()

spark.stop()
