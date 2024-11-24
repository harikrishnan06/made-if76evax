import unittest
import os
import sqlite3
import sys
import pandas as pd
import numpy as np
import stat

# Add parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from data_transform.trasform_gas_data import transform_and_store_data


class TestTransformAndStoreData(unittest.TestCase):

    def setUp(self):
        # Use relative test directory and database path
        self.test_dir = os.path.join(os.path.dirname(__file__), 'test_data')
        os.makedirs(self.test_dir, exist_ok=True)
        self.test_db_path = os.path.join(self.test_dir, 'test_gas_data.db')
        self.conn = None  # Initialize connection variable

    def tearDown(self):
        # Close any open SQLite connection
        if self.conn:
            self.conn.close()

        # Cleanup database and test directory
        if os.path.exists(self.test_db_path):
            try:
                os.remove(self.test_db_path)
            except PermissionError:
                print(f"Warning: Could not delete database file {self.test_db_path}. Ensure no open connections.")

        if os.path.exists(self.test_dir):
            os.chmod(self.test_dir, stat.S_IWUSR | stat.S_IRUSR)  # Restore write permissions if removed
            try:
                os.rmdir(self.test_dir)
            except OSError:
                print(f"Warning: Directory {self.test_dir} is not empty.")

    def generate_test_data(self, num_records=10):
        """
        Generates a test DataFrame with seeded random gasoline data.

        Parameters:
        - num_records: Number of rows in the generated data.

        Returns:
        - pandas DataFrame with random gasoline data.
        """
        np.random.seed(42)  # For reproducibility
        dates = pd.date_range(start='2023-01-01', periods=num_records, freq='W')
        prices = np.round(np.random.uniform(2.0, 5.0, num_records), 2)
        data = {
            'Date': dates,
            'Weekly U.S. All Grades All Formulations Retail Gasoline Prices  (Dollars per Gallon)': prices
        }
        return pd.DataFrame(data)

    def test_transform_and_store_data_success(self):
        """Test successful transformation and storage of gasoline data."""
        df = self.generate_test_data(10)
        transform_and_store_data(df, self.test_db_path)

        # Verify SQLite database
        self.conn = sqlite3.connect(self.test_db_path)
        result_df = pd.read_sql_query("SELECT * FROM gasoline_prices", self.conn)

        # Assertions
        self.assertEqual(len(result_df), 10)
        self.assertIn('timestamp', result_df.columns)
        self.assertIn('price', result_df.columns)

    def test_transform_and_store_data_missing_db_path(self):
        """Test handling of missing database directory."""
        df = self.generate_test_data(10)
        invalid_db_path = os.path.join(self.test_dir, 'non_existent_directory', 'test_gas_data.db')

        with self.assertRaises(FileNotFoundError):
            transform_and_store_data(df, invalid_db_path, strict=True)

    def test_transform_and_store_data_invalid_data_types(self):
        """Test handling of dataset with invalid data types."""
        df = pd.DataFrame({
            'Date': pd.date_range(start='2023-01-01', periods=10, freq='W'),
            'Weekly U.S. All Grades All Formulations Retail Gasoline Prices  (Dollars per Gallon)': ['invalid'] * 10
        })

        with self.assertRaises(ValueError):
            transform_and_store_data(df, self.test_db_path)


if __name__ == '__main__':
    unittest.main()
