# COMMAND ----------
import json
import os
import sys
import time
import uuid
import random
from datetime import datetime, timezone
import names
from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

def create_random_payment():
    """Generates a single random payment event."""
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "user_name": names.get_full_name(),
        "amount": round(random.uniform(1.0, 5000.0), 2),
        "currency": random.choice(["USD", "EUR", "GBP"]),
        "merchant_id": f"merchant_{random.randint(1, 100)}",
        # Generate ISO 8601 format timestamp with 'Z' for UTC
        "event_timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
    }

def main():
    """Continuously produces payment events to a Kafka topic."""
    # Determine the external Kafka bootstrap server address using the Minikube IP and the NodePort.
    # This is the correct way to connect from the host machine.
    try:
        minikube_ip = os.popen('minikube ip').read().strip()
        if not minikube_ip:
            raise RuntimeError("Minikube IP not found. Is Minikube running?")

        node_port_cmd = "kubectl get svc -n kafka my-kafka-cluster-kafka-external-bootstrap -o=jsonpath='{.spec.ports[0].nodePort}'"
        node_port = os.popen(node_port_cmd).read().strip()
        if not node_port:
            raise RuntimeError("Kafka NodePort not found. Check if the 'my-kafka-cluster-kafka-external-bootstrap' service exists in the 'kafka' namespace.")
    except Exception as e:
        print(f"FATAL: Could not determine Kafka connection details. {e}")
        sys.exit(1)
    
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", f"{minikube_ip}:{node_port}")
    topic_name = os.getenv("KAFKA_TOPIC_NAME", "paymentevents")

    def delivery_report(err, msg):
        """Called once for each message produced to indicate delivery result."""
        if err is not None:
            print(f"ERROR: Message delivery failed: {err}")

    # Common configuration for Producer and AdminClient
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'payment-producer'
    }

    try:
        # Create AdminClient to check for and create the topic
        admin_client = AdminClient(conf)
        print(f"Successfully connected to Kafka at {bootstrap_servers}")

        # Check if topic exists
        cluster_metadata = admin_client.list_topics(timeout=10)
        if topic_name not in cluster_metadata.topics:
            print(f"Topic '{topic_name}' not found. Creating it...")
            topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
            fs = admin_client.create_topics(topic_list)
            # Wait for each operation to finish.
            for topic, f in fs.items():
                f.result()  # The result itself is None on success
            print(f"Topic '{topic_name}' created.")
        else:
            print(f"Topic '{topic_name}' already exists.")

        # Create Producer
        producer = Producer(conf)

    except Exception as e:
        print(f"FATAL: Could not connect to Kafka brokers at {bootstrap_servers}.")
        print(f"Error: {e}")
        print("Please ensure Kafka is running and accessible.")
        sys.exit(1)

    print(f"\nProducing messages to Kafka topic: '{topic_name}'. Press Ctrl+C to stop.")
    try:
        while True:
            payment = create_random_payment()
            # Trigger any available delivery report callbacks from previous produce() calls
            producer.poll(0)
            # Asynchronously produce a message, the delivery report callback
            # will be triggered from a future poll() call.
            producer.produce(topic_name, json.dumps(payment).encode('utf-8'), callback=delivery_report)
            print(f"Sent: {payment['transaction_id']} - Amount: {payment['amount']} {payment['currency']}")
            time.sleep(random.uniform(0.5, 2.0))
    except KeyboardInterrupt:
        print("\nStopping producer.")
    finally:
        # Wait for any outstanding messages to be delivered and delivery report
        # callbacks to be triggered.
        producer.flush(30)
        print("Producer closed.")

if __name__ == "__main__":
    main()