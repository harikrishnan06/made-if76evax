import unittest
import os
import pandas as pd
import numpy as np
import sqlite3
import sys

# Add parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from data_transform.pre_process import process_gas_data, process_ev_data, merge_data, save_to_db

class TestPreProcess(unittest.TestCase):

    def setUp(self):
        # Setup test directory and file paths
        self.test_dir = './test_data'
        os.makedirs(self.test_dir, exist_ok=True)
        self.test_db_path = os.path.join(self.test_dir, 'test.db')
        self.test_gas_csv_path = os.path.join(self.test_dir, 'test_gas.csv')
        self.test_ev_csv_path = os.path.join(self.test_dir, 'test_ev.csv')
        self.test_merged_csv_path = os.path.join(self.test_dir, 'merged_data.csv')

    def tearDown(self):
        # Cleanup test directory and files
        if os.path.exists(self.test_db_path):
            os.remove(self.test_db_path)
        if os.path.exists(self.test_gas_csv_path):
            os.remove(self.test_gas_csv_path)
        if os.path.exists(self.test_ev_csv_path):
            os.remove(self.test_ev_csv_path)
        if os.path.exists(self.test_merged_csv_path):
            os.remove(self.test_merged_csv_path)
        if os.path.exists(self.test_dir):
            os.rmdir(self.test_dir)

    def generate_gas_data(self, inconsistent=False):
        # Generate sample gas data
        timestamps = pd.date_range(start='2020-01-01', periods=10, freq='7D')
        prices = np.random.rand(10) * 100
        if inconsistent:
            timestamps = timestamps[:-1]  # Remove the last timestamp to create inconsistency
            prices = prices[:-1]  # Ensure prices array is of the same length
        return pd.DataFrame({'timestamp': timestamps, 'price': prices})

    def generate_ev_data(self):
        # Generate sample EV data
        timestamps = pd.date_range(start='2020-01-01', periods=10, freq='M')
        volumes = np.random.randint(1, 100, size=10)
        return pd.DataFrame({'timestamp': timestamps, 'volume': volumes})

    def test_process_gas_data(self):
        # Test processing gasoline data for weekly consistency and monthly grouping
        gas_data = self.generate_gas_data(inconsistent=True)
        log_file = os.path.join(self.test_dir, 'log.txt')
        processed_gas_data = process_gas_data(gas_data, log_file)
        self.assertTrue(os.path.exists(log_file))
        self.assertIsInstance(processed_gas_data, pd.DataFrame)

    def test_process_ev_data(self):
        # Test processing EV data for monthly volumes
        ev_data = self.generate_ev_data()
        processed_ev_data = process_ev_data(ev_data)
        self.assertIsInstance(processed_ev_data, pd.DataFrame)

    def test_merge_data(self):
        # Test merging gas and EV data
        gas_data = self.generate_gas_data()
        ev_data = self.generate_ev_data()
        merged_data = merge_data(gas_data, ev_data, self.test_merged_csv_path, self.test_dir)
        self.assertTrue(os.path.exists(self.test_merged_csv_path))
        self.assertIsInstance(merged_data, pd.DataFrame)

    def test_save_to_db(self):
        # Test saving merged data to SQLite database
        gas_data = self.generate_gas_data()
        ev_data = self.generate_ev_data()
        merged_data = merge_data(gas_data, ev_data, self.test_merged_csv_path, self.test_dir)
        save_to_db(merged_data, self.test_db_path)
        self.assertTrue(os.path.exists(self.test_db_path))

if __name__ == '__main__':
    unittest.main()