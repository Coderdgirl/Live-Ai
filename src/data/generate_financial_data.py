import csv
import random
import datetime
import uuid
from pathlib import Path

# Define constants
OUTPUT_FILE = Path(__file__).parent / "samples" / "financial_anomalies.csv"
NUM_TRANSACTIONS = 100

# Transaction types
TRANSACTION_TYPES = ["withdrawal", "deposit", "purchase", "wire_transfer", "payment", "refund"]

# Merchants
MERCHANTS = [
    "ONLINE-RETAILER", "GROCERY-STORE", "GAS-STATION", "RESTAURANT", 
    "ELECTRONICS-STORE", "TRAVEL-AGENCY", "ATM-DOWNTOWN", "MOBILE-APP",
    "SUBSCRIPTION-SERVICE", "INTL-BANK-TRANSFER", "BRANCH-DOWNTOWN"
]

# Locations (lat, long)
LOCATIONS = [
    (40.7128, -74.0060),  # New York
    (34.0522, -118.2437),  # Los Angeles
    (41.8781, -87.6298),  # Chicago
    (29.7604, -95.3698),  # Houston
    (37.7749, -122.4194),  # San Francisco
    (51.5074, -0.1278),   # London
    (48.8566, 2.3522),    # Paris
    (35.6762, 139.6503),  # Tokyo
    (None, None)          # Online/No location
]

# Anomaly types
ANOMALY_TYPES = [
    "unusual_amount", 
    "unusual_location", 
    "unusual_time",
    "unusual_merchant", 
    "rapid_succession", 
    "unusual_destination",
    "unusual_deposit"
]

# Account numbers
ACCOUNT_NUMBERS = [f"ACC-{random.randint(1000, 9999)}" for _ in range(20)]

def generate_transaction(index):
    # Basic transaction data
    transaction_id = f"FIN{index+10:03d}"
    
    # Generate timestamp within the last 30 days
    days_ago = random.randint(0, 30)
    hours = random.randint(0, 23)
    minutes = random.randint(0, 59)
    seconds = random.randint(0, 59)
    
    timestamp = (datetime.datetime.now() - datetime.timedelta(days=days_ago, 
                                                             hours=random.randint(0, 12),
                                                             minutes=random.randint(0, 59))).replace(
        hour=hours, minute=minutes, second=seconds
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    account_number = random.choice(ACCOUNT_NUMBERS)
    
    # Determine if this will be an anomalous transaction (20% chance)
    is_anomaly = random.random() < 0.2
    
    # Generate amount (normal or anomalous)
    if is_anomaly and random.random() < 0.5:
        # Anomalous amount (very large or unusual)
        amount = round(random.uniform(5000, 50000), 2)
    else:
        # Normal amount
        amount = round(random.uniform(10, 2000), 2)
    
    transaction_type = random.choice(TRANSACTION_TYPES)
    merchant = random.choice(MERCHANTS)
    
    # Location
    lat, long = random.choice(LOCATIONS)
    
    # Determine anomaly details
    if is_anomaly:
        is_flagged = True
        anomaly_score = round(random.uniform(0.75, 0.98), 2)
        anomaly_type = random.choice(ANOMALY_TYPES)
    else:
        is_flagged = False
        anomaly_score = round(random.uniform(0.01, 0.4), 2)
        anomaly_type = ""
    
    return {
        "transaction_id": transaction_id,
        "timestamp": timestamp,
        "account_number": account_number,
        "amount": amount,
        "transaction_type": transaction_type,
        "merchant": merchant,
        "location": f"{lat},{long}" if lat and long else "",
        "is_flagged": str(is_flagged).lower(),
        "anomaly_score": anomaly_score,
        "anomaly_type": anomaly_type
    }

def main():
    # Define fieldnames consistently
    fieldnames = ["transaction_id", "timestamp", "account_number", "amount", 
                 "transaction_type", "merchant", "location", "is_flagged", 
                 "anomaly_score", "anomaly_type"]
    
    # Read existing data to avoid duplicating transaction IDs
    existing_data = []
    try:
        with open(OUTPUT_FILE, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            # Ensure we only keep the fields we need
            existing_data = []
            for row in reader:
                clean_row = {}
                for field in fieldnames:
                    clean_row[field] = row.get(field, "")
                existing_data.append(clean_row)
            print(f"Read {len(existing_data)} existing transactions")
    except Exception as e:
        print(f"Could not read existing data: {e}")
        # Create new file with headers
        with open(OUTPUT_FILE, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            existing_data = []
    
    # Generate new transactions
    new_transactions = [generate_transaction(i) for i in range(NUM_TRANSACTIONS)]
    
    # Combine existing and new data
    all_data = existing_data + new_transactions
    
    # Write all data back to CSV
    with open(OUTPUT_FILE, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in all_data:
            # Ensure we only write the fields we defined
            clean_row = {}
            for field in fieldnames:
                clean_row[field] = row.get(field, "")
            writer.writerow(clean_row)
    
    print(f"Successfully wrote {len(all_data)} transactions to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()