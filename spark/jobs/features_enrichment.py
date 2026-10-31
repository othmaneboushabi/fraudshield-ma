import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, hour, count, to_timestamp

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
    .appName("FraudShield_Features_Enrichment")
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
# 3. Fonction de traitement par lot (foreachBatch)
# --------------------------------------------------
def process_features(df_batch, batch_id):
    # Si le lot est vide, on passe
    if df_batch.isEmpty():
        return
        
    # 1. Conversion du texte en format Date/Heure (Timestamp)
    df_time = df_batch.withColumn("event_timestamp", to_timestamp(col("event_time")))
    
    # 2. Extraction simple de l'heure de la journée (0 à 23)
    df_features = df_time.withColumn("hour_of_day", hour(col("event_timestamp")))
    
    # 3. Calcul glissant : Nombre de transactions par carte dans ce lot
    # On partitionne par carte (card_id)
    window_spec = Window.partitionBy("card_id")
    df_enriched = df_features.withColumn("tx_count_current_batch", count("transaction_id").over(window_spec))
    
    # 4. Sauvegarde du résultat enrichi en format Delta
    (df_enriched.write
     .format("delta")
     .mode("append")
     .save("s3a://silver/transactions_features"))
    
    print(f"Lot {batch_id} traité et enrichi avec succès.")

# --------------------------------------------------
# 4. Lecture du flux Silver et application du calcul
# --------------------------------------------------
df_silver_clean = spark.readStream.format("delta").load("s3a://silver/transactions")

CHECKPOINT_PATH = "s3a://silver/checkpoints/features"

query = (
    df_silver_clean.writeStream
    .foreachBatch(process_features)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

print("Enrichissement des Features démarré. Ctrl+C pour arrêter.")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Arrêt demandé...")
    query.stop()
finally:
    spark.stop()