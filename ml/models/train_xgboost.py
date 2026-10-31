import os
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from deltalake import DeltaTable
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import precision_score, recall_score, f1_score, average_precision_score
import mlflow
import mlflow.xgboost

# --------------------------------------------------
# 1. Chargement de la configuration pour MinIO
# --------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")

# Le package 'deltalake' utilise les variables AWS standards
os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("MINIO_ROOT_USER", "admin")
os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("MINIO_ROOT_PASSWORD")
os.environ["AWS_ENDPOINT_URL"] = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ALLOW_HTTP"] = "true"

# --------------------------------------------------
# 2. Lecture des données Silver
# --------------------------------------------------
print("Chargement des données depuis MinIO (Silver)...")
dt = DeltaTable("s3://silver/transactions_features")
df = dt.to_pandas()

# --------------------------------------------------
# 3. Préparation des variables (Features)
# --------------------------------------------------
# On sélectionne les variables pertinentes pour l'IA
features = ["amount_mad", "hour_of_day", "tx_count_current_batch"]
X = df[features].fillna(0)
y = df["is_fraud"]

# Division en données d'entraînement (80%) et de test (20%)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# --------------------------------------------------
# 4. Entraînement et Suivi MLflow
# --------------------------------------------------
# Configuration de la base de données locale MLflow
mlflow.set_tracking_uri(f"sqlite:///{ROOT_DIR}/mlflow.db")
mlflow.set_experiment("FraudShield_XGBoost")

with mlflow.start_run():
    print("Entraînement du modèle XGBoost en cours...")
    # scale_pos_weight aide le modèle car les fraudes sont très rares (déséquilibre des classes)
    model = xgb.XGBClassifier(n_estimators=100, max_depth=3, scale_pos_weight=50, random_state=42)
    model.fit(X_train, y_train)

    # Prédictions sur l'échantillon de test
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    # Calcul des métriques exigées
    precision = precision_score(y_test, y_pred, zero_division=0)
    recall = recall_score(y_test, y_pred, zero_division=0)
    f1 = f1_score(y_test, y_pred, zero_division=0)
    pr_auc = average_precision_score(y_test, y_proba)

    # Sauvegarde des métriques et du modèle dans MLflow
    mlflow.log_metrics({"precision": precision, "recall": recall, "f1": f1, "pr_auc": pr_auc})
    mlflow.xgboost.log_model(model, "model", registered_model_name="FraudShield_Production")

    print(f"Modèle sauvegardé ! Précision: {precision:.2f} | Rappel: {recall:.2f} | F1: {f1:.2f} | PR-AUC: {pr_auc:.2f}")