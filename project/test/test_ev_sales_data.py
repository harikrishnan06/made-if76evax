import unittest
import os
import sqlite3
import pandas as pd
import numpy as np
import sys

# Add parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from data_transform.ev_sales_data import preprocess_ev_sales_data, save_to_sqlite


class TestEVSalesData(unittest.TestCase):

    def setUp(self):
        # Set up relative test directory and paths
        self.test_dir = os.path.join(os.path.dirname(__file__), 'test_data')
        os.makedirs(self.test_dir, exist_ok=True)
        self.test_db_path = os.path.join(self.test_dir, 'test_ev_sales.db')

    def tearDown(self):
    # Ensure SQLite database file is closed and cleaned up
        if os.path.exists(self.test_db_path):

            try:
                os.remove(self.test_db_path)
            except PermissionError:

                print(f"Warning: {self.test_db_path} could not be removed. Ensure all connections are closed.")
    
    # Clean up the test directory
        if os.path.exists(self.test_dir):
            try:
                os.rmdir(self.test_dir)
            except OSError:
                print(f"Warning: {self.test_dir} is not empty.")

    

    def generate_test_data(self, num_records=10):
        """
        Generates a test DataFrame with seeded random EV sales data.

        Parameters:
        - num_records: Number of rows in the generated data.

        Returns:
        - pandas DataFrame with random EV sales data.
        """
        np.random.seed(42)
        dates = pd.date_range(start='2023-01-01', periods=num_records, freq='M')
        vehicles = [f"Vehicle {i}" for i in range(num_records)]
        return pd.DataFrame({
            'Registration Valid Date': dates,
            'Vehicle Name': vehicles
        })

    def test_preprocess_ev_sales_data_success(self):
        """Test successful preprocessing of EV sales data."""
        df = self.generate_test_data(10)
        processed_df = preprocess_ev_sales_data(df)

        # Assertions
        self.assertEqual(len(processed_df), 10)
        self.assertIn('registration_date', processed_df.columns)
        self.assertIn('vehicle_name', processed_df.columns)

    def test_save_to_sqlite_success(self):
        """Test successful saving of preprocessed data to SQLite."""
        df = self.generate_test_data(10)
        processed_df = preprocess_ev_sales_data(df)
        save_to_sqlite(processed_df, self.test_db_path)

        # Verify SQLite database
        with sqlite3.connect(self.test_db_path) as conn:
            result_df = pd.read_sql_query("SELECT * FROM ev_sales", conn)

        # Assertions
        self.assertEqual(len(result_df), 10)
        self.assertIn('registration_date', result_df.columns)
        self.assertIn('vehicle_name', result_df.columns)

    def test_transform_with_invalid_data(self):
        """Test preprocessing with invalid data."""
        df = pd.DataFrame({
            'Registration Valid Date': [None] * 10,
            'Vehicle Name': [None] * 10
        })

        processed_df = preprocess_ev_sales_data(df)

        # Assertions
        self.assertEqual(len(processed_df), 0)  # All rows should be invalid


if __name__ == '__main__':
    unittest.main()
