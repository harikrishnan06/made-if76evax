import unittest
import os
import sys
from unittest.mock import patch, MagicMock
from requests.exceptions import RequestException

# Dynamically add the module path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from data_process.fetch_data import fetch_data_from_url

class TestFetchData(unittest.TestCase):

    def setUp(self):
        # Setup test directory and file path
        self.test_dir = './test_data'
        os.makedirs(self.test_dir, exist_ok=True)
        self.test_file_path = os.path.join(self.test_dir, 'test_file.txt')
        self.test_url = 'https://example.com/test_file.txt'

    def tearDown(self):
        # Cleanup test directory and files
        if os.path.exists(self.test_file_path):
            os.remove(self.test_file_path)
        if os.path.exists(self.test_dir):
            os.rmdir(self.test_dir)

    @patch('data_process.fetch_data.requests.get')  # Mock requests.get
    def test_file_download_success(self, mock_get):
        """Case 1: Test if file is downloaded and saved to the specified path."""
        # Mock successful response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"Mock file content"
        mock_get.return_value = mock_response

        # Call the function
        fetch_data_from_url(self.test_url, self.test_file_path)

        # Assert that the file exists in the specified path
        self.assertTrue(os.path.exists(self.test_file_path))

    @patch('data_process.fetch_data.requests.get')  # Mock requests.get
    def test_file_download_failure(self, mock_get):
        """Case 2: Test exception handling for an invalid URL."""
        # Mock a failed response
        mock_get.side_effect = RequestException("Mocked exception for invalid URL")

        # Capture logs
        with self.assertLogs(level='ERROR') as log:
            fetch_data_from_url("https://invalid-url.com", self.test_file_path)

        # Assert that the error message is logged
        self.assertTrue(any("Failed to fetch data due to a general request error" in message for message in log.output))

if __name__ == '__main__':
    unittest.main()
