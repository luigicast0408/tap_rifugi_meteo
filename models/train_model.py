from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder.appName("Advanced_Trainer").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. DATASET GENERATION
num_samples = 5000
df = spark.range(0, num_samples).select(
    (F.rand(42) * 50 - 15).alias("temperature"),
    (F.rand(43) * 100).alias("humidity"),
    (F.rand(44) * 120).alias("wind_speed"),
    (F.rand(46) * 23).cast("int").alias("hour")
)


# 2. CALCULATE INDEX OF RISK
train_df_raw = df.withColumn("label",
    F.when((F.col("wind_speed") > 80) |
           ((F.col("temperature") < -5) & (F.col("wind_speed") > 40)), 2)
     .when((F.col("wind_speed") > 50) |
           (F.col("temperature") < 0) |
           ((F.col("humidity") > 85) & (F.col("wind_speed") > 25)), 1)
     .otherwise(0)
)

(train_set, test_set) = train_df_raw.randomSplit([0.8, 0.2], seed=123)

# 3. PIPELINE ML
assembler = VectorAssembler(
    inputCols=["temperature", "humidity", "wind_speed", "hour"],
    outputCol="features_raw"
)

scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)

rf = RandomForestClassifier(
    labelCol="label",
    featuresCol="features",
    numTrees=100,
    maxDepth=10,
    seed=42
)

pipeline = Pipeline(stages=[assembler, scaler, rf])
model = pipeline.fit(train_set)

# 4. RATING
predictions = model.transform(test_set)
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)


MODEL_PATH = "/opt/spark/work-dir/models/saved_models/rf_weather_risk_model"
model.write().overwrite().save(MODEL_PATH)

print(f" Model saved on  {MODEL_PATH}")
spark.stop()