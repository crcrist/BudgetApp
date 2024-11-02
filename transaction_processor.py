import pandas as pd
import logging
import re
from datetime import datetime
import sqlite3

class TransactionProcessor:
    def __init__(self, db_connection, category_mappings):
        self.conn = db_connection
        self.cursor = db_connection.cursor()
        self.category_mappings = category_mappings

    def process_my_transaction(self, row):
        return {
            'transaction_id': row['Transaction ID'],
            'posting_date': row['Posting Date'],
            'effective_date': row['Effective Date'],
            'transaction_type': row['Transaction Type'],
            'amount': row['Amount'],
            'check_number': row['Check Number'],
            'reference_number': row['Reference Number'],
            'description': row['Description'],
            'transaction_category': self.categorize_description(row['Description']) or row['Transaction Category'],
            'type': row['Type'],
            'balance': row['Balance'],
            'memo': row['Memo'],
            'extended_description': row['Extended Description'],
            'account_owner': 'Connor'
        }

    def process_partner_transaction(self, row):
        try:
            # Parse MM/DD/YY format
            transaction_date = datetime.strptime(row['Transaction Date'], '%m/%d/%y')
            
            # Convert to M/D/YYYY format without using %-m and %-d
            month = str(transaction_date.month)  # This removes leading zeros
            day = str(transaction_date.day)      # This removes leading zeros
            year = transaction_date.strftime('%Y')
            formatted_date = f"{month}/{day}/{year}"
            
            unique_id = f"P_{transaction_date.strftime('%Y%m%d')}_{abs(hash(str(row['Transaction Description'])))}"
            
            processed_data = {
                'transaction_id': unique_id,
                'posting_date': formatted_date,
                'effective_date': formatted_date,
                'transaction_type': row['Transaction Type'],
                'amount': float(str(row['Transaction Amount']).replace(',', '')),
                'check_number': None,
                'reference_number': unique_id,
                'description': str(row['Transaction Description']),
                'transaction_category': self.categorize_description(str(row['Transaction Description'])),
                'type': row['Transaction Type'],
                'balance': float(str(row['Balance']).replace(',', '')),
                'memo': None,
                'extended_description': None,
                'account_owner': 'Partner'
            }
            
            logging.info(f"Successfully processed partner transaction: {unique_id}")
            return processed_data
            
        except Exception as e:
            logging.error(f"Error processing partner transaction: {e}")
            logging.error(f"Row data: {row}")
            raise

    def categorize_description(self, description):
        for pattern, category in self.category_mappings.items():
            if re.search(pattern, description, re.IGNORECASE):
                return category
        return 'Uncategorized'

    def process_csv(self, file_path, transaction_type):
        logging.info(f"Processing file: {file_path}")
        try:
            df = pd.read_csv(file_path)
            logging.info(f"Loaded {len(df)} rows from {file_path}")
            logging.info(f"CSV columns: {df.columns.tolist()}")

            if transaction_type == 'partner':
                required_columns = ['Account Number', 'Transaction Description', 'Transaction Date',
                                'Transaction Type', 'Transaction Amount', 'Balance']
                missing_columns = [col for col in required_columns if col not in df.columns]
                if missing_columns:
                    logging.error(f"Missing required columns: {missing_columns}")
                    return

            new_records = 0
            duplicate_records = 0

            for _, row in df.iterrows():
                if transaction_type == 'my':
                    processed_data = self.process_my_transaction(row)
                else:
                    processed_data = self.process_partner_transaction(row)

                self.cursor.execute('''
                    SELECT 1 FROM transactions 
                    WHERE transaction_id = ? AND reference_number = ?
                ''', (processed_data['transaction_id'], processed_data['reference_number']))

                if self.cursor.fetchone() is None:
                    try:
                        self.cursor.execute('''
                            INSERT INTO transactions (
                                transaction_id, posting_date, effective_date, transaction_type, 
                                amount, check_number, reference_number, description, 
                                transaction_category, type, balance, memo, extended_description,
                                account_owner
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', tuple(processed_data.values()))
                        self.conn.commit()
                        new_records += 1
                    except sqlite3.Error as e:
                        logging.error(f"Error inserting record {processed_data['transaction_id']}: {e}")
                else:
                    duplicate_records += 1

            logging.info(f"Completed processing {file_path}. New records: {new_records}, Duplicates: {duplicate_records}")

        except Exception as e:
            logging.error(f"Failed to load or process CSV file {file_path}: {e}")
            return