import os
from pathlib import Path
from dotenv import load_dotenv
import streamlit as st
import pandas as pd
from deltalake import DeltaTable
import time

# --------------------------------------------------
# 1. Configuration de la page et des variables
# --------------------------------------------------
st.set_page_config(
    page_title="FraudShield MA - Dashboard",
    page_icon="🛡️",
    layout="wide"
)

ROOT_DIR = Path(__file__).resolve().parent
load_dotenv(ROOT_DIR / ".env")

# Configuration AWS pour lire MinIO (Gold)
os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("MINIO_ROOT_USER", "admin")
os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("MINIO_ROOT_PASSWORD")
os.environ["AWS_ENDPOINT_URL"] = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ALLOW_HTTP"] = "true"

# --------------------------------------------------
# 2. Fonction de chargement des données Gold
# --------------------------------------------------
@st.cache_data(ttl=5) # Rafraîchit les données toutes les 5 secondes
def load_gold_data():
    try:
        dt = DeltaTable("s3a://gold/transactions_scored")
        return dt.to_pandas()
    except Exception as e:
        return pd.DataFrame()

# --------------------------------------------------
# 3. Interface Graphique (Streamlit)
# --------------------------------------------------
st.title("🛡️ FraudShield MA : Surveillance des Fraudes en Temps Réel")
st.markdown("Tableau de bord de monitoring des transactions bancaires marocaines (Architecture Medallion + Spark + IA).")

# Bouton de rafraîchissement manuel
if st.button("🔄 Rafraîchir les données"):
    st.cache_data.clear()

df = load_gold_data()

if df.empty:
    st.warning("Aucune donnée trouvée dans la couche Gold pour le moment. Assurez-vous que vos scripts Spark et le générateur tournent !")
else:
    # Calcul des KPIs principaux
    total_tx = len(df)
    total_fraud = int(df["is_predicted_fraud"].sum())
    fraud_rate = (total_fraud / total_tx) * 100 if total_tx > 0 else 0
    total_amount = df["amount_mad"].sum()

    # Affichage des métriques en haut de page
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Transactions", f"{total_tx:,}")
    col2.metric("Fraudes Détectées", f"{total_fraud:,}", delta_color="inverse")
    col3.metric("Taux de Fraude", f"{fraud_rate:.2f}%")
    col4.metric("Volume Total (MAD)", f"{total_amount:,.2f} MAD")

    st.divider()

    # Graphiques et tableaux en deux colonnes
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("📊 Répartition des montants (MAD)")
        st.line_chart(df.set_index("event_time")["amount_mad"] if "event_time" in df.columns else df["amount_mad"])

    with col_right:
        st.subheader("🚨 Dernières alertes de fraude détectées")
        fraud_df = df[df["is_predicted_fraud"] == 1]
        if not fraud_df.empty:
            st.dataframe(fraud_df[["transaction_id", "card_id", "amount_mad", "fraud_probability", "city"]].tail(10), use_container_width=True)
        else:
            st.success("Aucune fraude critique détectée pour l'instant dans ce lot.")

    # Tableau global complet
    st.subheader("📜 Historique complet des transactions scorées")
    st.dataframe(df, use_container_width=True)