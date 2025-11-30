# COMMAND ----------
import json
import random
import time
import uuid
from datetime import datetime

import names
from kafka import KafkaProducer


def create_random_payment():
    """Generates a single random payment event."""
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "user_name": names.get_full_name(),
        "amount": round(random.uniform(1.0, 5000.0), 2),
        "currency": random.choice(["USD", "EUR", "GBP"]),
        "merchant_id": f"merchant_{random.randint(1, 100)}",
        "event_timestamp": datetime.utcnow().isoformat() + "Z",
        
    }


def main():
    """Continuously produces payment events to a Kafka topic."""
    # In a containerized setup, you might use a service name like 'my-kafka.kafka.svc.cluster.local:9092'
    # For local testing, 'localhost:9092' is common if you've port-forwarded Kafka.
    bootstrap_servers = "localhost:9092"
    topic_name = "payment-events_test"

    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5, # Retry sending on failure
        )
        print(f"Successfully connected to Kafka at {bootstrap_servers}")
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        return

    print(f"Producing messages to Kafka topic: '{topic_name}'. Press Ctrl+C to stop.")
    try:
        while True:
            payment = create_random_payment()
            producer.send(topic_name, value=payment)
            print(f"Sent: {payment['transaction_id']}")
            time.sleep(random.uniform(0.5, 3.0))  # Simulate variable time between events
    except KeyboardInterrupt:
        print("\nStopping producer.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()