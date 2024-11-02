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

# Add account_owner column if it doesn't exist
try:
    cursor.execute('''
    ALTER TABLE transactions
    ADD COLUMN account_owner TEXT DEFAULT 'Connor'
    ''')
    conn.commit()
except sqlite3.OperationalError:
    # Column already exists
    pass

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

# Get folder paths
my_csv_folder = os.getenv("MY_CSV_FOLDER")
partner_csv_folder = os.getenv("PARTNER_CSV_FOLDER")

# Add debug logging for paths
logging.info(f"My CSV folder path: {my_csv_folder}")
logging.info(f"Partner CSV folder path: {partner_csv_folder}")

# Verify folders exist
if not os.path.exists(my_csv_folder):
    logging.error(f"My CSV folder not found: {my_csv_folder}")
    exit()
if not os.path.exists(partner_csv_folder):
    logging.error(f"Partner CSV folder not found: {partner_csv_folder}")
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

# Verify results
cursor.execute("SELECT COUNT(*) FROM transactions WHERE account_owner = 'Partner'")
partner_count = cursor.fetchone()[0]
logging.info(f"Total partner transactions in database: {partner_count}")

cursor.execute("SELECT COUNT(*) FROM transactions WHERE account_owner = 'Connor'")
my_count = cursor.fetchone()[0]
logging.info(f"Total my transactions in database: {my_count}")

# Close the connection
try:
    cursor.close()
    conn.close()
    logging.info("Database connection closed.")
except sqlite3.Error as e:
    logging.error(f"Error closing database connection: {e}")