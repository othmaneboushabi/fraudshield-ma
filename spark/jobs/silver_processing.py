import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, from_json

# --------------------------------------------------
# 1. Chargement de la configuration
# --------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

# --------------------------------------------------
# 2. Création de la session Spark
# --------------------------------------------------
builder = (
    SparkSession.builder
    .appName("FraudShield_Silver_Processing")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
)

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# 3. Définition du schéma JSON de la transaction
# --------------------------------------------------
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("client_id", StringType(), True),
    StructField("card_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("amount_mad", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("city", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("bank", StringType(), True),
    StructField("is_fraud", IntegerType(), True)
])

# --------------------------------------------------
# 4. Lecture de Bronze, Transformation et Écriture dans Silver
# --------------------------------------------------
# Lecture en streaming depuis la couche Bronze
df_bronze = spark.readStream.format("delta").load("s3a://bronze/transactions")

# Typage et extraction des colonnes depuis le JSON binaire
df_parsed = (
    df_bronze
    .withColumn("json_string", col("value").cast("string"))
    .withColumn("data", from_json(col("json_string"), schema))
    .select("data.*", col("timestamp").alias("kafka_timestamp"))
)

# Nettoyage : Suppression des doublons avec un Watermark de 10 minutes
df_cleaned = (
    df_parsed
    .withWatermark("kafka_timestamp", "10 minutes")
    .dropDuplicates(["transaction_id"])
)

# Écriture dans la couche Silver
SILVER_PATH = "s3a://silver/transactions"
CHECKPOINT_PATH = "s3a://silver/checkpoints/transactions"

query = (
    df_cleaned.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start(SILVER_PATH)
)

print("Traitement Silver démarré. Ctrl+C pour arrêter.")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Arrêt demandé...")
    query.stop()
finally:
    spark.stop()