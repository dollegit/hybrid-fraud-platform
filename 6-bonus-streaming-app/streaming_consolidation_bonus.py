# COMMAND ----------
import time
import random
from datetime import datetime, timedelta
import pandas as pd

# --- Data Simulation (Generators for Streams) ---

def payment_stream_generator(accounts, num_payments=20):
    """Simulates a live API stream of payment transactions."""
    for i in range(num_payments):
        src, dest = random.sample(accounts, 2)
        yield {
            'payment_id': f'pay_{70000 + i}',
            'src_account_id': src,
            'dest_account_id': dest,
            'amount': round(random.uniform(10.0, 100.0), 2),
            'timestamp': datetime.utcnow()
        }
        time.sleep(random.uniform(0.1, 0.3)) # Simulate network latency

def fraud_stream_generator(payment_ids, fraud_ratio=0.3):
    """Simulates a delayed stream of fraud labels."""
    fraudulent_payments = random.sample(payment_ids, k=int(len(payment_ids) * fraud_ratio))
    for pay_id in fraudulent_payments:
        yield {
            'payment_id': pay_id,
            'fraud_type': 'APP Fraud',
            'fraud_reported_date': (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
        }
        time.sleep(random.uniform(0.5, 1.0)) # Simulate delay and network latency

# --- Batch/Reference Data Loading ---

def load_reference_data():
    """
    Simulates loading daily/irregular batch data into memory.
    In a real system, this would read from a DB or file system.
    """
    # Simulating account_details.csv
    accounts = [
        {'account_id': f'acc_{1000 + i}', 'opening_date': '2022-01-15'} for i in range(50)
    ]
    account_details = pd.DataFrame(accounts).set_index('account_id')

    # Simulating external_risk_feed.csv
    risky_accounts = [
        {'account_id': random.choice(account_details.index), 'risk_flag': 1} for _ in range(5)
    ]
    risk_feed = pd.DataFrame(risky_accounts).set_index('account_id')

    return account_details, risk_feed

# --- Streaming Processor ---

def process_streams(payment_stream, fraud_stream, account_details, risk_feed, wait_timeout_seconds=3):
    """
    Designs a processor to handle streaming and batch data.

    This function simulates a stateful stream processor.
    """
    
    # State: A "waiting room" for payments waiting for potential fraud labels.
    # Key: payment_id, Value: enriched payment data
    unmatched_payments = {}
    finalized_records = []

    payment_gen = payment_stream
    fraud_gen = fraud_stream

    print("Starting stream processing... (Will run for a few seconds)")

    while True:
        try:
            # Process a payment from the stream
            payment = next(payment_gen)
            print(f"  -> Received Payment: {payment['payment_id']}")

            # Enrich with reference data immediately
            payment['src_opening_date'] = account_details.loc[payment['src_account_id']]['opening_date']
            payment['dest_opening_date'] = account_details.loc[payment['dest_account_id']]['opening_date']
            
            # Use .get() with a default for risk flags, as not all accounts are risky
            payment['src_risk_flag'] = risk_feed.get('risk_flag', {}).get(payment['src_account_id'], 0)
            payment['dest_risk_flag'] = risk_feed.get('risk_flag', {}).get(payment['dest_account_id'], 0)

            # Add to our stateful "waiting room"
            unmatched_payments[payment['payment_id']] = payment

        except StopIteration:
            # Payment stream finished, now just process remaining fraud events
            pass

        try:
            # Process a fraud event from the stream
            fraud_event = next(fraud_gen)
            print(f"  -> Received Fraud Label for: {fraud_event['payment_id']}")
            
            if fraud_event['payment_id'] in unmatched_payments:
                # Found a match, enrich and finalize
                payment_to_finalize = unmatched_payments.pop(fraud_event['payment_id'])
                payment_to_finalize['fraud_flag'] = True
                payment_to_finalize.update(fraud_event)
                finalized_records.append(payment_to_finalize)
            else:
                # Fraud label arrived for a payment we haven't seen or already timed out.
                # In a real system, this might be stored for a "late join" or flagged.
                print(f"     (Warning: Fraud label for unknown/timed-out payment {fraud_event['payment_id']})")

        except StopIteration:
            # Both streams are finished, break the loop
            if not unmatched_payments:
                break

        # Timeout check (Watermarking simulation)
        # Check for payments that have been waiting too long and finalize them as non-fraudulent.
        now = datetime.utcnow()
        timed_out_ids = [
            pid for pid, p in unmatched_payments.items() if (now - p['timestamp']).total_seconds() > wait_timeout_seconds
        ]

        for pid in timed_out_ids:
            print(f"  -> TIMEOUT for payment {pid}. Finalizing as non-fraud.")
            payment_to_finalize = unmatched_payments.pop(pid)
            payment_to_finalize['fraud_flag'] = False
            payment_to_finalize['fraud_type'] = None
            payment_to_finalize['fraud_reported_date'] = None
            finalized_records.append(payment_to_finalize)
        
        if not unmatched_payments and 'payment' not in locals(): # End condition
            break

    # Add any remaining payments in the waiting room as non-fraudulent
    for pid, p in unmatched_payments.items():
        p['fraud_flag'] = False
        p['fraud_type'] = None
        p['fraud_reported_date'] = None
        finalized_records.append(p)

    print("\nStream processing complete.")
    return pd.DataFrame(finalized_records)

if __name__ == "__main__":
    # 1. Load reference data (simulates daily/batch load)
    account_details_df, risk_feed_df = load_reference_data()
    
    # 2. Setup stream simulators
    all_account_ids = account_details_df.index.tolist()
    num_stream_payments = 20
    
    # We need payment IDs ahead of time to simulate the fraud stream referencing them
    simulated_payment_ids = [f'pay_{70000 + i}' for i in range(num_stream_payments)]
    
    payment_gen = payment_stream_generator(all_account_ids, num_stream_payments)
    fraud_gen = fraud_stream_generator(simulated_payment_ids)

    # 3. Run the processor
    final_df = process_streams(payment_gen, fraud_gen, account_details_df, risk_feed_df)

    print("\n--- Final Consolidated Table (from Streams) ---")
    print(final_df)