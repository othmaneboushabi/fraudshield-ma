import json
import time
import random
import uuid
from datetime import datetime, timezone
from faker import Faker
from confluent_kafka import Producer
import os
from dotenv import load_dotenv

# Charge les variables du fichier .env situé dans le dossier parent
load_dotenv(dotenv_path="../.env")

# Configuration Faker et Kafka
fake = Faker()
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")

# Initialisation du Producteur Kafka
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

# Données de référence pour le Maroc
CITIES = [
    {"city": "Casablanca", "lat": 33.5731, "lon": -7.5898},
    {"city": "Rabat", "lat": 34.0209, "lon": -6.8416},
    {"city": "Marrakech", "lat": 31.6295, "lon": -8.0369},
    {"city": "Fes", "lat": 34.0331, "lon": -5.0003},
    {"city": "Tangier", "lat": 35.7595, "lon": -5.8340}
]
BANKS = ["Attijariwafa", "BCP", "Bank of Africa", "CIH", "Credit Agricole"]
CHANNELS = ["CARD", "MOBILE_TRANSFER", "ATM"]

def delivery_report(err, msg):
    """Fonction appelée une fois le message envoyé à Kafka."""
    if err is not None:
        print(f"Erreur de livraison: {err}")
    else:
        pass # Message envoyé avec succès

def generate_transaction():
    """Génère une transaction JSON aléatoire avec injection possible de fraude."""
    location = random.choice(CITIES)
    is_fraud = 1 if random.random() < 0.02 else 0 # 2% de fraude injectée
    
    # Scénario de fraude : montant anormalement élevé
    amount = round(random.uniform(5000, 50000), 2) if is_fraud else round(random.uniform(10, 2000), 2)

    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "client_id": f"C-{random.randint(1000, 9999)}",
        "card_id": f"CARD-{random.randint(100, 999)}",
        "device_id": f"DEV-{random.randint(10, 99)}",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "amount_mad": amount,
        "currency": "MAD",
        "channel": random.choice(CHANNELS),
        "merchant_id": f"M-{random.randint(1, 500)}",
        "merchant_category": fake.word(),
        "city": location["city"],
        "lat": location["lat"],
        "lon": location["lon"],
        "bank": random.choice(BANKS),
        "is_fraud": is_fraud
    }
    return transaction

if __name__ == "__main__":
    print(f"Démarrage du générateur vers Kafka ({KAFKA_BROKER}), topic: {KAFKA_TOPIC}")
    try:
        while True:
            txn = generate_transaction()
            # Envoi du message JSON converti en texte (encode utf-8)
            producer.produce(
                KAFKA_TOPIC, 
                value=json.dumps(txn).encode('utf-8'), 
                callback=delivery_report
            )
            producer.poll(0) # Vide la file d'attente
            print(f"Envoyé: {txn['transaction_id']} | Montant: {txn['amount_mad']} MAD | Fraude: {txn['is_fraud']}")
            
            time.sleep(random.uniform(0.1, 1.0)) # Pause entre chaque transaction
    except KeyboardInterrupt:
        print("\nArrêt du générateur.")
    finally:
        producer.flush()