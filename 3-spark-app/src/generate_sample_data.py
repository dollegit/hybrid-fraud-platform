import pandas as pd
import random
from datetime import datetime, timedelta
import os
from pyspark.sql import SparkSession

def generate_sample_data(num_payments=1000, num_accounts=200):
    """Generates and saves four sample CSV files for the consolidation task."""
    print("Generating sample data...")
    # Define the output directory which is the mounted PVC in the Spark pod.
    # This path is consistent with the volumeMount in the SparkApplication YAML.
    output_dir = os.getenv("DATA_OUTPUT_PATH", "/opt/spark/data")

    # --- Account Details ---
    accounts = []
    for i in range(num_accounts):
        accounts.append({
            'account_id': f'acc_{1000 + i}',
            'opening_date': (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1500))).strftime('%Y-%m-%d')
        })
    accounts_df = pd.DataFrame(accounts)
    output_path = os.path.join(output_dir, 'account_details.csv')
    accounts_df.to_csv(output_path, index=False)
    print(f"Generated {len(accounts_df)} account details -> account_details.csv")

    account_ids = accounts_df['account_id'].tolist()

    # --- Payment Transactions ---
    payments = []
    for i in range(num_payments):
        src, dest = random.sample(account_ids, 2)
        payments.append({
            'payment_id': f'pay_{50000 + i}',
            'src_account_id': src,
            'dest_account_id': dest,
            'payment_reference': f'ref_{random.randint(10000, 99999)}',
            'amount': round(random.uniform(1.0, 5000.0), 2),
            'timestamp': (datetime.now() - timedelta(minutes=random.randint(1, 100000))).isoformat()
        })
    payments_df = pd.DataFrame(payments)
    output_path = os.path.join(output_dir, 'payment_transactions.csv')
    payments_df.to_csv(output_path, index=False)
    print(f"Generated {len(payments_df)} payments -> payment_transactions.csv")

    # --- External Risk Feed ---
    risky_accounts = random.sample(account_ids, k=int(num_accounts * 0.15))
    risk_feed = []
    for acc_id in risky_accounts:
        risk_feed.append({
            'account_id': acc_id,
            'risk_flag': random.choice([1, 2]) # 1=suspicious, 2=confirmed
        })
    risk_feed_df = pd.DataFrame(risk_feed)
    output_path = os.path.join(output_dir, 'external_risk_feed.csv')
    risk_feed_df.to_csv(output_path, index=False)
    print(f"Generated {len(risk_feed_df)} risk entries -> external_risk_feed.csv")

    # --- Historical Fraud Cases ---
    fraudulent_payments = random.sample(payments_df['payment_id'].tolist(), k=int(num_payments * 0.05))
    fraud_cases = []
    for pay_id in fraudulent_payments:
        fraud_cases.append({
            'payment_id': pay_id,
            'fraud_type': random.choice(['Money Mule', 'APP Fraud', 'Identity Theft']),
            'fraud_reported_date': (datetime.now() - timedelta(days=random.randint(30, 90))).strftime('%Y-%m-%d')
        })
    fraud_cases_df = pd.DataFrame(fraud_cases)
    output_path = os.path.join(output_dir, 'historical_fraud_cases.csv')
    fraud_cases_df.to_csv(output_path, index=False)
    print(f"Generated {len(fraud_cases_df)} fraud cases -> historical_fraud_cases.csv")

    print("\nSample data generation complete.")
    
def main():
    """
    Main entry point for the Spark job.
    Initializes a SparkSession, runs the data generation, and then stops the session.
    """
    spark = SparkSession.builder.appName("GenerateSampleData").getOrCreate()

    print("🎉 SparkSession created. Starting data generation...")

    generate_sample_data()

    print("🛑 Stopping SparkSession.")
    spark.stop()

if __name__ == "__main__":
    main()