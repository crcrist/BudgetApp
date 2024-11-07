import os
import logging
import sqlite3
import json
from dotenv import load_dotenv
from transaction_processor import TransactionProcessor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

# Load environment variables
load_dotenv()

# Expand paths from environment variables
db_path = os.path.expanduser(os.getenv("DB_PATH"))
my_csv_folder = os.path.expanduser(os.getenv("MY_PATH"))
partner_csv_folder = os.path.expanduser(os.getenv("BBY_PATH"))
card_csv_folder = os.path.expanduser(os.getenv("CARD_PATH"))


# Database connection
try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    logging.info(f"Connected to the database at {db_path}")
except sqlite3.Error as e:
    logging.error(f"Database connection failed: {e}")
    exit()

# Create transactions table if it doesn't exist
try:
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id TEXT PRIMARY KEY,
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
        account_owner TEXT
    )
    ''')
    conn.commit()
    logging.info("Transactions table verified/created.")
except sqlite3.Error as e:
    logging.error(f"Error creating transactions table: {e}")
    exit()

# Load category mappings
category_file = 'transaction_categories.json'
try:
    with open(category_file, 'r') as f:
        category_mappings = json.load(f)
    logging.info("Loaded category mappings from JSON.")
except FileNotFoundError:
    logging.error(f"Category mapping file {category_file} not found.")
    exit()

# Initialize processor
processor = TransactionProcessor(conn, category_mappings)

# Add debug logging for paths
logging.info(f"My CSV folder path: {my_csv_folder}")
logging.info(f"Partner CSV folder path: {partner_csv_folder}")
logging.info(f"Card CSV folder path: {card_csv_folder}")

# Verify folders exist
if not os.path.exists(my_csv_folder):
    logging.error(f"My CSV folder not found: {my_csv_folder}")
    exit()
if not os.path.exists(partner_csv_folder):
    logging.error(f"Partner CSV folder not found: {partner_csv_folder}")
    exit()
if not os.path.exists(card_csv_folder):
    logging.error(f"Card CSV folder not found: {card_csv_folder}")
    exit()

# Process my transactions
my_files = os.listdir(my_csv_folder)
logging.info(f"Found my files: {my_files}")
for filename in my_files:
    if filename.endswith('.csv'):
        file_path = os.path.join(my_csv_folder, filename)
        processor.process_csv(file_path, 'my')

# Process partner transactions
partner_files = os.listdir(partner_csv_folder)
logging.info(f"Found partner files: {partner_files}")
for filename in partner_files:
    if filename.endswith('.csv'):
        file_path = os.path.join(partner_csv_folder, filename)
        processor.process_csv(file_path, 'partner')

card_files = os.listdir(card_csv_folder)
logging.info(f"Found card files: {card_files}")
for filename in card_files:
    if filename.endswith('.csv'):
        file_path = os.path.join(card_csv_folder, filename)
        processor.process_csv(file_path, 'card')

# Verify results
cursor.execute("SELECT COUNT(*) FROM transactions WHERE account_owner = 'Partner'")
partner_count = cursor.fetchone()[0]
logging.info(f"Total partner transactions in database: {partner_count}")

cursor.execute("SELECT COUNT(*) FROM transactions WHERE account_owner = 'Connor'")
my_count = cursor.fetchone()[0]
logging.info(f"Total my transactions in database: {my_count}")

cursor.execute("SELECT COUNT(*) FROM transactions WHERE account_owner = 'Card'")
card_count = cursor.fetchone()[0]
logging.info(f"Total card transactions in databse: {card_count}")

# Close the connection
try:
    cursor.close()
    conn.close()
    logging.info("Database connection closed.")
except sqlite3.Error as e:
    logging.error(f"Error closing database connection: {e}")
