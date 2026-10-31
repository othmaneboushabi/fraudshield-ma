# 🛡️ FraudShield MA : Real-Time Moroccan Banking Fraud Detection Platform

<p align="center">
  <img src="https://img.shields.io/badge/Architecture-Medallion%20Lakehouse-blue?style=for-the-badge&logo=databricks" alt="Medallion">
  <img src="https://img.shields.io/badge/Stream-Apache%20Spark%20%2F%20Kafka-red?style=for-the-badge&logo=apache-spark" alt="Spark">
  <img src="https://img.shields.io/badge/Storage-MinIO%20%2F%20Delta%20Lake-yellow?style=for-the-badge&logo=minio" alt="MinIO">
  <img src="https://img.shields.io/badge/ML-XGBoost%20%2F%20MLflow-orange?style=for-the-badge&logo=mlflow" alt="MLflow">
  <img src="https://img.shields.io/badge/UI-Streamlit-success?style=for-the-badge&logo=streamlit" alt="Streamlit">
</p>

---

## 📌 À Propos du Projet

**FraudShield MA** est une plateforme de pointe en temps réel conçue pour détecter les fraudes sur les transactions bancaires marocaines. Elle s'appuie sur une stack **Big Data & Data Engineering** moderne combinant une architecture **Medallion Lakehouse (Bronze, Silver, Gold)**, du traitement de flux en direct (Streaming) et un modèle de Machine Learning supervisé.

---

## 🏗️ Architecture Technique

```mermaid
graph LR
    A["Générateur de Transactions"] -->|"Kafka / Redpanda"| B("Bronze : Données Brutes")
    B -->|"Spark Streaming"| C("Silver : Nettoyage & Features")
    C -->|"XGBoost & MLflow"| D("Gold : Scoring & Alertes")
    D --> E["Streamlit Dashboard"]
    B -.-> S[("MinIO / Delta Lake")]
    C -.-> S
    D -.-> S
    S -.-> E
```

- **Ingestion** : Redpanda (compatible Kafka) pour le streaming de transactions.
- **Stockage & Lakehouse** : MinIO (S3 compatible) structuré en couches Bronze, Silver et Gold au format Delta Lake.
- **Traitement & Calcul** : Apache Spark Structured Streaming pour l'ETL et l'enrichissement comportemental (fenêtres glissantes).
- **Intelligence Artificielle** : Modèle de classification XGBoost entraîné et suivi via MLflow.
- **Visualisation** : Tableau de bord interactif en temps réel sous Streamlit.

---

## 📊 Aperçu du Tableau de Bord (Streamlit)

Le dashboard offre un suivi complet des opérations bancaires et des alertes de fraude critiques :

- **KPIs en direct** : Volume total, nombre de fraudes, taux de fraude global (%).
- **Monitoring temporel** : Visualisation des flux et des pics de montants en Dirhams Marocains (MAD).
- **Registre d'alertes** : Affichage instantané des transactions suspectes avec probabilités d'anomalie.

---

## ⚙️ Guide d'Installation et Lancement

### 1. Prérequis

- Docker Desktop installé et actif.
- Python 3.10+ configuré dans un environnement virtuel.

### 2. Démarrer l'infrastructure Docker

```powershell
docker-compose up -d
```

### 3. Activer l'environnement virtuel et installer les dépendances

```powershell
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
```

### 4. Lancer les pipelines Spark & ML

Exécuter les scripts dans cet ordre :

1. **Générateur de flux** : `python spark/jobs/transaction_generator.py`
2. **Ingestion Bronze** : `python spark/jobs/bronze_ingestion.py`
3. **Traitement Silver** : `python spark/jobs/silver_processing.py`
4. **Enrichissement (Features)** : `python spark/jobs/features_enrichment.py`
5. **Entraînement ML** : `python ml/models/train_xgboost.py`
6. **Scoring Gold (Temps Réel)** : `python spark/jobs/gold_scoring.py`

### 5. Lancer l'interface utilisateur (Streamlit)

```powershell
streamlit run app.py
```

---

## 👨‍💻 Auteur

**Othmane Boushabi** - Étudiant en Ingénierie Data & Intelligence Artificielle (EMSI Rabat)
