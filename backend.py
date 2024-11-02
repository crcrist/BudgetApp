import os
import pandas as pd
import sqlite3
import logging
import json
import re
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

# Load environment
load_dotenv()

# Database connection
db_path = os.getenv("DB_PATH")

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    logging.info("Connected to the database.")
except sqlite3.Error as e:
    logging.error(f"Database connection failed: {e}")
    exit()

# Create the transactions table if it does not exist
try:
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id TEXT,
        posting_date TEXT,
        effective_date TEXT,
        transaction_type TEXT,
        amount REAL,
        check_number TEXT,
        reference_number TEXT,
        description TEXT,
        transaction_category TEXT,
        type TEXT,
        balance REAL,
        memo TEXT,
        extended_description TEXT,
        UNIQUE(transaction_id, reference_number)
    )
    ''')
    conn.commit()
    logging.info("Verified the transactions table exists or created a new one.")
except sqlite3.Error as e:
    logging.error(f"Failed to create or verify transactions table: {e}")
    exit()

# Load category mappings from JSON
category_file = 'transaction_categories.json'
try:
    with open(category_file, 'r') as f:
        category_mappings = json.load(f)
    logging.info("Loaded category mappings from JSON.")
except FileNotFoundError:
    logging.error(f"Category mapping file {category_file} not found.")
    exit()
except json.JSONDecodeError as e:
    logging.error(f"Error parsing JSON file {category_file}: {e}")
    exit()

# Function to categorize transaction descriptions
def categorize_description(description):
    for pattern, category in category_mappings.items():
        if re.search(pattern, description, re.IGNORECASE):
            return category
    return None

def process_csv(file_path):
    logging.info(f"Processing file: {file_path}")
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Loaded {len(df)} rows from {file_path}")
    except Exception as e:
        logging.error(f"Failed to load CSV file {file_path}: {e}")
        return

    # Process each transaction
    new_records = 0
    duplicate_records = 0
    for _, row in df.iterrows():
        transaction_id = row['Transaction ID']
        reference_number = row['Reference Number']

        # Check if the transaction is already in the database
        cursor.execute('''
        SELECT 1 FROM transactions
        WHERE transaction_id = ? AND reference_number = ?
        ''', (transaction_id, reference_number))
        
        if cursor.fetchone() is None:
            # Categorize description
            category = categorize_description(row['Description'])
            if not category:
                category = row['Transaction Category']  # Default to existing category if no match

            # Insert new transaction
            try:
                cursor.execute('''
                INSERT INTO transactions (
                    transaction_id, posting_date, effective_date, transaction_type, amount, check_number,
                    reference_number, description, transaction_category, type, balance, memo, extended_description
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    transaction_id, row['Posting Date'], row['Effective Date'], row['Transaction Type'], row['Amount'],
                    row['Check Number'], reference_number, row['Description'], category,
                    row['Type'], row['Balance'], row['Memo'], row['Extended Description']
                ))
                conn.commit()
                new_records += 1
            except sqlite3.Error as e:
                logging.error(f"Error inserting record {transaction_id}: {e}")
        else:
            duplicate_records += 1

    logging.info(f"Completed processing {file_path}. New records added: {new_records}. Duplicate records skipped: {duplicate_records}.")

# Directory containing CSV files
csv_folder = os.getenv("CSV_FOLDER")

# Process each CSV file in the folder
for filename in os.listdir(csv_folder):
    if filename.endswith('.csv'):
        file_path = os.path.join(csv_folder, filename)
        process_csv(file_path)

# Close the connection
try:
    cursor.close()
    conn.close()
    logging.info("Database connection closed.")
except sqlite3.Error as e:
    logging.error(f"Error closing database connection: {e}")
