import os
import pandas as pd
import sqlite3
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def transform_and_store_data(df, db_path, strict=True):
    """
    Transforms gasoline data and stores it in an SQLite database.

    Parameters:
    - df: pandas DataFrame containing the raw gasoline data.
    - db_path: Relative path to the SQLite database file.
    - strict: If True, raises exceptions for invalid data types or missing directories.
    """
    try:
        logging.info("Starting transformation of gasoline data.")

        # Validate columns
        expected_columns = ['Date', 'Weekly U.S. All Grades All Formulations Retail Gasoline Prices  (Dollars per Gallon)']
        if not all(col in df.columns for col in expected_columns):
            raise KeyError(f"Missing required columns. Expected columns: {expected_columns}")

        # Rename the columns
        df = df.rename(columns={
            'Date': 'timestamp',
            'Weekly U.S. All Grades All Formulations Retail Gasoline Prices  (Dollars per Gallon)': 'price'
        })

        # Validate data types
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            raise ValueError("Column 'Date' must contain datetime values.")
        if not pd.api.types.is_numeric_dtype(df['price']):
            raise ValueError("Column 'price' must contain numeric values.")

        # Correct data types
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df['price'] = pd.to_numeric(df['price'], errors='coerce')

        # Drop rows with invalid data
        df = df.dropna()

        # Convert timestamp to string format
        df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

        # Validate directory path
        dir_path = os.path.dirname(db_path)
        if not os.path.exists(dir_path):
            if strict:
                raise FileNotFoundError(f"Directory {dir_path} does not exist.")
            os.makedirs(dir_path)

        # Check write permissions
        if not os.access(dir_path, os.W_OK):
            raise PermissionError(f"No write permissions for directory: {dir_path}")

        # Save to SQLite
        conn = sqlite3.connect(db_path)
        try:
            df.to_sql('gasoline_prices', conn, if_exists='replace', index=False)
            logging.info(f"Data successfully saved to SQLite database at {db_path}")
        finally:
            conn.close()
    except (FileNotFoundError, PermissionError, ValueError, KeyError) as e:
        logging.error(f"Error during transformation: {e}")
        raise
