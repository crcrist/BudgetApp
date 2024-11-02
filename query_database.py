import sqlite3
import logging
import os
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

# Load env
load_dotenv()

db_path = os.getenv("DB_PATH")

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    logging.info("Connected to the database.")
except sqlite3.Error as e:
    logging.error(f"Database connection failed: {e}")
    exit()

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

def select_query():
    cursor.execute("SELECT DISTINCT count(distinct(posting_date))  FROM transactions WHERE amount < 0 ")
    rows = cursor.fetchall()

    print ("All combinations of description and category:")
    for row in rows:
        print(row)


# Example usage
# show_all_transactions()  # Display all transactions
# show_transactions_by_id("20241028 56253 8,198 3,222")  # Display transactions by ID
# count_transactions()  # Display total count of transactions
# check_for_duplicates()
# delete_db()
select_query()

# Close the connection when done
cursor.close()
conn.close()
