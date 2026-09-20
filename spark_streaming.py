from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

spark = (
    SparkSession.builder
    .appName("mobility-streaming")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("vehicle_id", StringType()),
    StructField("vehicle_label", StringType()),
    StructField("trip_id", StringType()),
    StructField("route_id", StringType()),
    StructField("direction_id", IntegerType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("bearing", DoubleType()),
    StructField("speed_kmh", DoubleType()),
    StructField("odometer", DoubleType()),
    StructField("stop_sequence", IntegerType()),
    StructField("status", StringType()),
    StructField("stop_id", StringType()),
    StructField("timestamp", StringType()),
])

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "vehicle_positions")
    .option("startingOffsets", "latest")
    .load()
)

events = (
    raw_stream
    .select(
        from_json(
            col("value").cast("string"),
            schema,
        ).alias("data")
    )
    .select("data.*")
)

route_stats = (
    events
    .groupBy("route_id")
    .agg(
        count("*").alias("event_count"),
        avg("speed_kmh").alias("avg_speed_kmh"),
    )
)


def write_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    import psycopg2

    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        database="mobility",
        user="mobility",
        password="mobility",
    )

    cursor = conn.cursor()
    rows = batch_df.collect()

    for row in rows:
        cursor.execute(
            """
            INSERT INTO route_speed_history (
                route_id,
                event_count,
                avg_speed_kmh,
                processed_at
            )
            VALUES (%s, %s, %s, NOW())
            """,
            (
                row["route_id"],
                row["event_count"],
                row["avg_speed_kmh"],
            ),
        )

        cursor.execute(
            """
            INSERT INTO route_speed_metrics (
                route_id,
                event_count,
                avg_speed_kmh
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (route_id)
            DO UPDATE SET
                event_count = EXCLUDED.event_count,
                avg_speed_kmh = EXCLUDED.avg_speed_kmh,
                processed_at = NOW()
            """,
            (
                row["route_id"],
                row["event_count"],
                row["avg_speed_kmh"],
            ),
        )

    conn.commit()
    cursor.close()
    conn.close()

    print(
        f"Batch {batch_id} écrit : "
        f"{len(rows)} lignes dans l'historique"
    )


query = (
    route_stats
    .writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("complete")
    .start()
)

query.awaitTermination()
