from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date, hour, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, ArrayType
)

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaToHDFS-Weather-Raw") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Define Nested Schema (Matches Open-Meteo structure)
# This handles the 'current' and 'hourly' blocks in the JSON
schema = StructType([
    StructField("source", StringType(), True),
    StructField("city", StringType(), True),
    StructField("current", StructType([
        StructField("time", StringType(), True),
        StructField("temperature_2m", DoubleType(), True),
        StructField("wind_speed_10m", DoubleType(), True)
    ])),
    StructField("hourly", StructType([
        StructField("temperature_2m", ArrayType(DoubleType()), True)
    ]))
])

# 3. Read from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "10.0.0.94:9092,10.0.0.96:9092") \
    .option("subscribe", "weather_raw") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# 4. Parse JSON + FLATTEN
parsed_df = raw_df \
    .selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), schema).alias("weather")) \
    .select(
        col("weather.city").alias("city"),
        col("weather.current.time").alias("event_time"),
        col("weather.current.temperature_2m").alias("temperature_2m"),
        col("weather.current.wind_speed_10m").alias("wind_speed_10m")
    ) \
    .withColumn("event_ts", to_timestamp(col("event_time"))) \
    .withColumn("date", to_date(col("event_ts"))) \
    .withColumn("hour", hour(col("event_ts")))

# 5. Write to HDFS (Parquet)
query = parsed_df.writeStream \
    .format("parquet") \
    .partitionBy("date", "hour") \
    .option("path", "hdfs://10.0.0.62:9000/raw/weather/") \
    .option("checkpointLocation", "hdfs://10.0.0.62:9000/checkpoints/weather_raw/") \
    .outputMode("append") \
    .start()

query.awaitTermination()
