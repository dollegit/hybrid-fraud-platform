# COMMAND ----------
import json
import time
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer
import names

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
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    topic_name = "paymentevents"
    print(f"Producing messages to Kafka topic: '{topic_name}'. Press Ctrl+C to stop.")

    try:
        while True:
            payment = create_random_payment()
            producer.send(topic_name, value=payment)
            print(f"Sent: {payment['transaction_id']} - Amount: {payment['amount']} {payment['currency']}")
            time.sleep(random.uniform(0.5, 2.0))
    except KeyboardInterrupt:
        print("\nStopping producer.")
    finally:
        producer.flush()
        producer.close()
        print("Producer closed.")

if __name__ == "__main__":
    main()