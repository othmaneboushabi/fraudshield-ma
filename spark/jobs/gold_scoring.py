
import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
import mlflow
import mlflow.xgboost
import pandas as pd

# --------------------------------------------------
# 1. Chargement de la configuration
# --------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

# Configuration MLflow : même base que train_Xgboost.py
MLFLOW_DB = f"sqlite:///{(ROOT_DIR / 'mlflow.db').as_posix()}"

mlflow.set_tracking_uri(MLFLOW_DB)
mlflow.set_registry_uri(MLFLOW_DB)

# Configuration MinIO
os.environ["AWS_ACCESS_KEY_ID"] = MINIO_ACCESS_KEY
os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET_KEY
os.environ["AWS_ENDPOINT_URL"] = MINIO_ENDPOINT
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ALLOW_HTTP"] = "true"

# --------------------------------------------------
# 2. Création de la session Spark
# --------------------------------------------------
builder = (
    SparkSession.builder
    .appName("FraudShield_Gold_Scoring")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
)

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# 3. Chargement du modèle MLflow
# --------------------------------------------------
model_uri = "models:/FraudShield_Production/1"

print(f"Chargement du modèle depuis : {model_uri}")

model = mlflow.xgboost.load_model(model_uri)

print("Modèle XGBoost chargé avec succès !")

# --------------------------------------------------
# 4. Fonction de scoring par lot
# --------------------------------------------------
def score_batch(df_batch, batch_id):

    if df_batch.isEmpty():
        return

    pdf = df_batch.toPandas()

    if pdf.empty:
        return

    features = [
        "amount_mad",
        "hour_of_day",
        "tx_count_current_batch"
    ]

    X = pdf[features].fillna(0)

    pdf["fraud_probability"] = model.predict_proba(X)[:, 1]

    pdf["is_predicted_fraud"] = (
        pdf["fraud_probability"] >= 0.5
    ).astype(int)

    scored_df = spark.createDataFrame(pdf)

    (
        scored_df.write
        .format("delta")
        .mode("append")
        .save("s3a://gold/transactions_scored")
    )

    print(
        f"Lot {batch_id} scoré avec succès "
        "-> Enregistré dans Gold."
    )

# --------------------------------------------------
# 5. Lecture du flux Silver
# --------------------------------------------------
df_features_stream = (
    spark.readStream
    .format("delta")
    .load("s3a://silver/transactions_features")
)

CHECKPOINT_PATH = "s3a://gold/checkpoints/scoring"

query = (
    df_features_stream.writeStream
    .foreachBatch(score_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

print("Scoring en temps réel (Couche Gold) démarré.")

try:
    query.awaitTermination()

except KeyboardInterrupt:
    print("Arrêt demandé...")
    query.stop()

finally:
    spark.stop()
