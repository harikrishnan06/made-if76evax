import os
import pandas as pd
import requests
import sqlite3

def download_file(url, local_path):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        with open(local_path, 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved to {local_path}")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
        raise

def read_and_process_excel(file_path):
    try:
        # Read the Excel file, skipping the first two rows
        df = pd.read_excel(file_path, sheet_name='Data 1', skiprows=2)
        
        # Rename the columns
        df.rename(columns={
            'Date': 'timestamp',
            'Weekly U.S. All Grades All Formulations Retail Gasoline Prices  (Dollars per Gallon)': 'price'
        }, inplace=True)
        
        # Correct data types
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        
        # Convert timestamp to string format
        df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        print("Excel file read and processed successfully.")
        return df
    except Exception as e:
        print(f"Error reading or processing Excel file: {e}")
        raise

def save_to_sqlite(df, db_path):
    try:
        # Ensure the directory exists
        print(db_path)
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Connect to SQLite database (or create it if it doesn't exist)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS prices (
                id INTEGER PRIMARY KEY,
                timestamp TEXT,
                price REAL
            )
        ''')
        
        # Insert data into the table
        for index, row in df.iterrows():
            cursor.execute('''
                INSERT INTO prices (timestamp, price) VALUES (?, ?)
            ''', (row['timestamp'], row['price']))
        
        # Commit the transaction and close the connection
        conn.commit()
        conn.close()
        
        print(f"Data saved to SQLite database at {db_path}")
    except sqlite3.Error as e:
        print(f"Error saving data to SQLite: {e}")
        raise

def fetch_data():
    url = 'https://www.eia.gov/dnav/pet/hist_xls/EMM_EPM0_PTE_NUS_DPGw.xls'
    local_file_path = os.path.join(os.path.dirname(__file__), '../../data/gasoline_price.xls')
    db_path = os.path.join(os.path.dirname(__file__), '../../data/gasoline_prices.db')
    
    try:
        download_file(url, local_file_path)
        df = read_and_process_excel(local_file_path)
        save_to_sqlite(df, db_path)
        print("Data fetching and processing completed successfully.")
    except Exception as e:
        print(f"An error occurred in the fetch_data pipeline: {e}")

if __name__ == "__main__":
    fetch_data()