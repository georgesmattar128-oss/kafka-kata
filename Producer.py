import json
import time
import requests
from datetime import datetime, timezone
from kafka import KafkaProducer
from config import KAFKA_BROKER, KAFKA_TOPIC, API_URL, FETCH_INTERVAL_SECONDS


def create_producer():
    """Crée et retourne un producteur Kafka."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def fetch_rates():
    """
    Appelle l'API externe et retourne un dict structuré :
    {
        "timestamp": "2025-04-21T10:00:00+00:00",
        "base": "USD",
        "rates": { "EUR": 0.91, "GBP": 0.78, ... }
    }
    Retourne None en cas d'erreur.
    """
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "base": data.get("base", "USD"),
            "rates": data.get("rates", {}),
        }

    except requests.exceptions.RequestException as e:
        print(f"[ERREUR] Impossible de contacter l'API : {e}")
        return None


def publish(producer, message):
    """Publie un message sur le topic Kafka."""
    future = producer.send(KAFKA_TOPIC, value=message)
    producer.flush()
    metadata = future.get(timeout=10)
    print(
        f"[OK] Publié sur '{metadata.topic}' "
        f"partition={metadata.partition} offset={metadata.offset}"
    )


def main():
    print(f"Démarrage du producteur — broker: {KAFKA_BROKER} | topic: {KAFKA_TOPIC}")
    print(f"Intervalle : {FETCH_INTERVAL_SECONDS}s\n")

    producer = create_producer()

    try:
        while True:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Récupération des taux...")

            rates = fetch_rates()

            if rates:
                publish(producer, rates)
                print(f"  Base  : {rates['base']}")
                print(f"  Devises reçues : {len(rates['rates'])}")
            else:
                print("[SKIP] Aucune donnée publiée ce cycle.")

            print(f"  Prochain appel dans {FETCH_INTERVAL_SECONDS}s...\n")
            time.sleep(FETCH_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nArrêt du producteur (Ctrl+C).")
    finally:
        producer.close()
        print("Connexion Kafka fermée.")


if __name__ == "__main__":
    main()