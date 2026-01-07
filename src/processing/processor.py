from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml import PipelineModel

# 1. INITIALIZE SPARK
spark = SparkSession.builder.appName("Hut_Weather_Risk_Final_Fix").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 2. LOAD PRE-TRAINED MODEL
model = PipelineModel.load("/opt/spark/work-dir/models/saved_models/rf_weather_risk_model")

# 3. LOAD STATIC METADATA
huts_metadata = spark.read.json("/opt/spark/work-dir/data/huts.json").select(
    F.lower(F.trim(F.col("name"))).alias("join_name"),
    F.col("lat").alias("h_lat"),
    F.col("lon").alias("h_lon"),
    F.col("region")
)

# 4. DEFINE SCHEMA
weather_schema = StructType([
    StructField("station_name", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("wind_speed", DoubleType()),
    StructField("timestamp", StringType())
])

# 5. READ FROM KAFKA
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "meteo") \
    .option("startingOffsets", "latest") \
    .option("kafkaConsumer.pollTimeoutMs", "120000") \
    .load()

# 6. DATA PARSING & DEDUPLICATION
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING) AS raw") \
    .withColumn("data", F.from_json(F.col("raw"), weather_schema)) \
    .select("data.*") \
    .filter(F.col("station_name").isNotNull()) \
    .dropDuplicates(["station_name", "timestamp"])

# 7. JOIN & FEATURE PREP
enriched_stream = parsed_stream.withColumn("st_low", F.lower(F.trim(F.col("station_name")))) \
    .join(huts_metadata, F.col("st_low") == F.col("join_name"), "left") \
    .withColumn("hour", F.hour(F.coalesce(F.to_timestamp(F.col("timestamp")), F.current_timestamp()))) \
    .withColumn("day_of_year", F.dayofyear(F.current_timestamp())) \
    .na.fill({"temperature": 0.0, "humidity": 50.0, "wind_speed": 5.0})

# 8. Daylight Window
enriched_stream = enriched_stream.withColumn("estimated_sunset",
    F.lit(18.5) - F.lit(2.2) * F.cos(F.lit(2 * 3.14159) * (F.col("day_of_year") + F.lit(10)) / F.lit(365))
)

enriched_stream = enriched_stream.withColumn("daylight_remaining",
    F.round(F.col("estimated_sunset") - F.col("hour"), 1)
)

enriched_stream = enriched_stream.withColumn("daylight_status",
    F.when(F.col("daylight_remaining") > 2.0, "Abundant Light")
     .when(F.col("daylight_remaining").between(0.5, 2.0), "Twilight")
     .when(F.col("daylight_remaining") < 0.5, "Darkness / Night")
     .otherwise("Sufficient Light")
)

# 9. CALCOLATE  WIND CHILL AND  ICE PROBABILTY
enriched_stream = enriched_stream.withColumn("wind_chill",
    F.round(F.lit(13.12) + (F.lit(0.6215) * F.col("temperature")) -
    (F.lit(11.37) * F.pow(F.col("wind_speed"), 0.16)) +
    (F.lit(0.3965) * F.col("temperature") * F.pow(F.col("wind_speed"), 0.16)), 1)
).withColumn("ice_probability",
    F.when((F.col("temperature").between(-2.0, 2.0)) & (F.col("humidity") > 80), "High")
     .when((F.col("temperature") < -2.0) & (F.col("humidity") > 70), "Medium")
     .otherwise("No Risk")
)

# 10. RUN INFERENCE
predictions = model.transform(enriched_stream)

# 11. PERSON TARGET & DYNAMIC ADVICE
final_predictions = predictions.withColumn("target_user",
    F.when((F.col("prediction") == 0) & (F.col("wind_speed") < 25) & (F.col("temperature") > 8), "Family and tourists")
     .when((F.col("prediction") <= 1) & (F.col("wind_speed") < 50) & (F.col("temperature") > -2), "Prepared hikers")
     .otherwise("Experts / Mountaineers Only")
).withColumn("hypothermia_risk",
    F.when(F.col("wind_chill") >= 0, "No Risk").when(F.col("wind_chill") >= -10, "Low").otherwise("Moderate/High")
)

final_predictions = final_predictions.withColumn("advice",
    F.when((F.col("daylight_remaining") < 1.5) & (F.col("prediction") >= 1), "CRITICAL: Worsening weather & darkness imminent!")
     .when(F.col("daylight_remaining") < 1.0, "Urgent: Less than 1 hour of light left.")
     .when(F.col("prediction") == 2, "Danger: Extreme weather conditions.")
     .otherwise("Conditions stable for the selected target.")
)

# 12. OUTPUT SELECTION
final_output = final_predictions.select(
    F.col("station_name").alias("hut_name"),
    "temperature", "humidity", "wind_speed", "wind_chill", "region",
    "ice_probability", "hypothermia_risk", "daylight_remaining", "daylight_status",
    F.col("prediction").cast("int").alias("weather_risk_index"),
    "target_user", "advice",
    F.concat_ws(",", F.col("h_lat"), F.col("h_lon")).alias("location"),
    F.current_timestamp().alias("@timestamp")
)

# 13. SINK TO ELASTICSEARCH
checkpoint_path = "/opt/spark/work-dir/data/checkpoints/v_final_full_intelligence"

query = final_output.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .option("es.resource", "huts_weather_risk") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .option("es.nodes.wan.only", "true") \
    .option("es.index.auto.create", "true") \
    .start()

print(" Pipeline Active...")
query.awaitTermination()