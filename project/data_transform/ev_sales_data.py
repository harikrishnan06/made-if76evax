import os
import pandas as pd
import sqlite3
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def preprocess_ev_sales_data(df):
    logging.info("Starting data preprocessing.")
    required_columns = ['Registration Valid Date', 'Vehicle Name']
    if not all(col in df.columns for col in required_columns):
        raise ValueError(f"The expected columns {required_columns} are missing from the CSV file.")

    df = df.rename(columns={
        'Registration Valid Date': 'registration_date',
        'Vehicle Name': 'vehicle_name'
    })
    df['registration_date'] = pd.to_datetime(df['registration_date'], errors='coerce')
    df = df.dropna()
    logging.info("Data preprocessing completed successfully.")
    return df


def save_to_sqlite(df, db_path):
    logging.info("Saving data to SQLite database.")
    dir_path = os.path.dirname(db_path)
    if not os.path.exists(dir_path):
        try:
            os.makedirs(dir_path)
        except OSError as e:
            logging.error(f"Failed to create directory {dir_path}: {e}")
            raise

    try:
        with sqlite3.connect(db_path) as conn:
            df.to_sql('ev_sales', conn, if_exists='replace', index=False)
        logging.info(f"Data saved to SQLite database at {db_path}")
    except Exception as e:
        logging.error(f"Error saving data to SQLite database: {e}")
        raise


def fetch_and_preprocess_ev_sales(csv_file_path, db_path):
    try:
        logging.info(f"Reading data from CSV file: {csv_file_path}")
        df = pd.read_csv(csv_file_path)
        processed_df = preprocess_ev_sales_data(df)
        save_to_sqlite(processed_df, db_path)
        logging.info("Data fetching, preprocessing, and saving to database completed successfully.")
    except Exception as e:
        logging.error(f"Error in the fetch_and_preprocess_ev_sales pipeline: {e}")
        raise
