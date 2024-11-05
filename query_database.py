import sqlite3
import logging
import os
from pathlib import Path
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

# Load environment variables
load_dotenv()

# Get paths from environment variables
DB_PATH = os.path.expanduser(os.getenv('DB_PATH'))
BBY_PATH = os.path.expanduser(os.getenv('BBY_PATH'))
MY_PATH = os.path.expanduser(os.getenv('MY_PATH'))

# Validate environment variables
if not all([DB_PATH, BBY_PATH, MY_PATH]):
    logging.error("Missing required environment variables. Please check your .env file")
    exit(1)

# Ensure directories exist
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(BBY_PATH, exist_ok=True)
os.makedirs(MY_PATH, exist_ok=True)

try:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    logging.info(f"Connected to the database at {DB_PATH}")
except sqlite3.Error as e:
    logging.error(f"Database connection failed: {e}")
    exit(1)

# Function to display all transactions
def show_all_transactions():
    cursor.execute("SELECT * FROM transactions")
    rows = cursor.fetchall()
    
    print("All Stored Transactions:")
    for row in rows:
        print(row)

# Function to show transactions by specific Transaction ID
def show_transactions_by_id(transaction_id):
    cursor.execute("SELECT * FROM transactions WHERE transaction_id = ?", (transaction_id,))
    rows = cursor.fetchall()
    
    print(f"Transactions with Transaction ID '{transaction_id}':")
    for row in rows:
        print(row)

# Function to count total transactions in the database
def count_transactions():
    cursor.execute("SELECT COUNT(*) FROM transactions")
    count = cursor.fetchone()[0]
    print(f"Total number of transactions: {count}")

def check_for_duplicates():
    duplicate_query = """
        SELECT transaction_id, reference_number, COUNT(*) as count
        FROM transactions
        GROUP BY transaction_id, reference_number
        HAVING count > 1
    """
    
    cursor.execute(duplicate_query)
    duplicates = cursor.fetchall()
    if duplicates:
        print("Duplicate Records Found:")
        for row in duplicates:
            print(f"Transaction ID: {row[0]}, Reference Number: {row[1]}, Count: {row[2]}")
    else:
        print("No duplicates found.")

def delete_db():
    delete_query = "DELETE FROM transactions"
    cursor.execute(delete_query)
    conn.commit()
    print("All transactions have been deleted from the database.")

def update_db():
    try:
        cursor.execute('''
        ALTER TABLE transactions 
        ADD COLUMN account_owner TEXT DEFAULT 'Connor'
        ''')
        conn.commit()
        print("Database has been updated")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e).lower():
            print("Column 'account_owner' already exists")
        else:
            raise e

def select_query():
    # First, get column names
    cursor.execute("PRAGMA table_info(transactions)")
    columns = [column[1] for column in cursor.fetchall()]
    
    query = """
    SELECT * FROM (
        SELECT * FROM transactions 
        WHERE account_owner = 'Partner'
        ORDER BY transaction_id DESC
        LIMIT 2
    )
    UNION ALL
    SELECT * FROM (
        SELECT * FROM transactions 
        WHERE account_owner = 'Connor'
        ORDER BY transaction_id DESC
        LIMIT 2
    )
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    
    print("\nColumn names:")
    for i, col in enumerate(columns):
        print(f"{i}: {col}")
    
    print("\nTop 2 most recent transactions for each account owner:")
    for row in rows:
        print("\nTransaction:")
        for col_name, value in zip(columns, row):
            print(f"{col_name}: {value}")
        print("-" * 50)

def get_transaction_files():
    """Get all transaction files from both directories"""
    bby_files = list(Path(BBY_PATH).glob('*'))
    my_files = list(Path(MY_PATH).glob('*'))
    return {
        'bby': bby_files,
        'my': my_files
    }

if __name__ == "__main__":
    try:
        # show_all_transactions()  # Display all transactions
        # show_transactions_by_id("20241028 56253 8,198 3,222")  # Display transactions by ID
        # count_transactions()  # Display total count of transactions
        # check_for_duplicates()
        # delete_db()
        # update_db()
        select_query()
        
        # Get all transaction files
        transaction_files = get_transaction_files()
        logging.info(f"Found {len(transaction_files['bby'])} BBY transaction files")
        logging.info(f"Found {len(transaction_files['my'])} MY transaction files")
    finally:
        # Close the connection when done
        cursor.close()
        conn.close()
        logging.info("Database connection closed")
