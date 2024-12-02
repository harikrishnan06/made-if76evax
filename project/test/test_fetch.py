import pytest
import os
import sys
from unittest.mock import patch, MagicMock
from requests.exceptions import Timeout, ConnectionError, HTTPError, RequestException

# Add the parent directory to sys.path to import fetch_data_from_url
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from data_process.fetch_data import fetch_data_from_url, download_file

import requests
 
 
 
@pytest.fixture
def setup_test_environment(tmp_path):
    """Fixture to set up a temporary environment for testing."""
    test_file_path = tmp_path / "test_file.csv"
    return test_file_path

def test_valid_url_download(setup_test_environment):
    """Test downloading a file from a valid URL."""
    test_file_path = setup_test_environment
    test_url = "https://www.atlasevhub.com/public/dmv/wa_ev_registrations_public.csv"

    # Call the function
    fetch_data_from_url(test_url, str(test_file_path))

    # Assert file is created
    assert os.path.exists(test_file_path)

def test_invalid_url_exception(setup_test_environment):
    """Test handling of an invalid URL."""
    test_file_path = setup_test_environment
    test_url = "https://invalid-url.com/non_existent_file.csv"

    # Assert exception is raised for invalid URL
    with pytest.raises(Exception):
        fetch_data_from_url(test_url, str(test_file_path))

def test_timeout_exception(setup_test_environment, monkeypatch):
    """Test handling of a timeout error."""
    test_file_path = setup_test_environment
    test_url = "https://www.example.com/timeout-test"

    # Mock a timeout exception
    def mock_request_get(*args, **kwargs):
        raise requests.exceptions.Timeout("Mocked timeout error")
    
    monkeypatch.setattr("requests.get", mock_request_get)

    # Assert timeout exception is raised
    with pytest.raises(requests.exceptions.Timeout):
        fetch_data_from_url(test_url, str(test_file_path))

def test_connection_error_exception(setup_test_environment, monkeypatch):
    """Test handling of a connection error."""
    test_file_path = setup_test_environment
    test_url = "https://www.example.com/connection-test"

    # Mock a connection error exception
    def mock_request_get(*args, **kwargs):
        raise requests.exceptions.ConnectionError("Mocked connection error")

    monkeypatch.setattr("requests.get", mock_request_get)

    # Assert connection error exception is raised
    with pytest.raises(requests.exceptions.ConnectionError):
        fetch_data_from_url(test_url, str(test_file_path))

def test_http_error_exception(setup_test_environment, monkeypatch):
    """Test handling of an HTTP error."""
    test_file_path = setup_test_environment
    test_url = "https://www.example.com/http-error-test"

    # Mock an HTTP error exception
    def mock_request_get(*args, **kwargs):
        raise requests.exceptions.HTTPError("Mocked HTTP error")

    monkeypatch.setattr("requests.get", mock_request_get)

    # Assert HTTP error exception is raised
    with pytest.raises(requests.exceptions.HTTPError):
        fetch_data_from_url(test_url, str(test_file_path))

def test_general_request_exception(setup_test_environment, monkeypatch):
    """Test handling of a general request exception."""
    test_file_path = setup_test_environment
    test_url = "https://www.example.com/general-request-error-test"

    # Mock a general request exception
    def mock_request_get(*args, **kwargs):
        raise requests.exceptions.RequestException("Mocked general request exception")

    monkeypatch.setattr("requests.get", mock_request_get)

    # Assert general request exception is raised
    with pytest.raises(requests.exceptions.RequestException):
        fetch_data_from_url(test_url, str(test_file_path))
