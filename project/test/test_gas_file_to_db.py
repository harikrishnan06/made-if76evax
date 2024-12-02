import os
import pytest
import pandas as pd
import sqlite3
import sys

# Add parent directory to sys.path to import the function
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from data_transform.trasform_gas_data import transform_and_store_data

@pytest.fixture
def setup_environment(tmp_path):
    """Setup temporary paths for testing."""
    db_path = tmp_path / "test_db.sqlite"
    return db_path

def test_transform_and_store_success(setup_environment):
    """Test successful transformation and storage of valid dataset."""
    db_path = setup_environment

    # Generate a valid synthetic dataset
    data = {
        "Date": pd.date_range(start="2023-01-01", periods=5, freq="W"),
        "Weekly U.S. All Grades All Formulations Retail Gasoline Prices  (Dollars per Gallon)": [3.5, 3.6, 3.7, 3.8, 3.9]
    }
    df = pd.DataFrame(data)

    # Transform and store the data
    transform_and_store_data(df, str(db_path))

    # Verify the SQLite database content
    conn = sqlite3.connect(db_path)
    result_df = pd.read_sql_query("SELECT * FROM gasoline_prices", conn)
    conn.close()

    assert not result_df.empty, "Transformed data is empty."
    assert "timestamp" in result_df.columns, "Column 'timestamp' missing in database."
    assert "price" in result_df.columns, "Column 'price' missing in database."
    assert len(result_df) == 5, "Expected 5 rows in the database."

def test_transform_and_store_missing_columns(setup_environment):
    """Test transformation with missing columns."""
    db_path = setup_environment

    # Create a DataFrame with missing columns
    data = {
        "WrongColumn1": [1, 2, 3],
        "WrongColumn2": ["invalid", "data", "test"]
    }
    df = pd.DataFrame(data)

    # Expect a KeyError for missing required columns
    with pytest.raises(KeyError, match="Missing required columns"):
        transform_and_store_data(df, str(db_path))

def test_transform_and_store_invalid_data_types(setup_environment):
    """Test transformation with invalid data types."""
    db_path = setup_environment

    # Create a DataFrame with invalid types
    data = {
        "Date": ["invalid_date1", "invalid_date2", "invalid_date3"],
        "Weekly U.S. All Grades All Formulations Retail Gasoline Prices  (Dollars per Gallon)": ["invalid", "data", "test"]
    }
    df = pd.DataFrame(data)

    # Expect a ValueError for invalid datetime and numeric types
    with pytest.raises(ValueError, match="Column 'Date' must contain datetime values"):
        transform_and_store_data(df, str(db_path))

def test_transform_and_store_missing_directory(setup_environment):
    """Test handling of missing directory."""
    # Define a non-existent directory
    invalid_db_path = os.path.join("non_existent_dir", "test_db.sqlite")

    # Generate a valid dataset
    data = {
        "Date": pd.date_range(start="2023-01-01", periods=5, freq="W"),
        "Weekly U.S. All Grades All Formulations Retail Gasoline Prices  (Dollars per Gallon)": [3.5, 3.6, 3.7, 3.8, 3.9]
    }
    df = pd.DataFrame(data)

    # Expect a FileNotFoundError if the directory is missing and strict=True
    with pytest.raises(FileNotFoundError, match="does not exist"):
        transform_and_store_data(df, invalid_db_path, strict=True)

def test_transform_and_store_permission_error(setup_environment, monkeypatch):
    """Test handling of write permission error."""
    db_path = setup_environment

    # Generate a valid dataset
    data = {
        "Date": pd.date_range(start="2023-01-01", periods=5, freq="W"),
        "Weekly U.S. All Grades All Formulations Retail Gasoline Prices  (Dollars per Gallon)": [3.5, 3.6, 3.7, 3.8, 3.9]
    }
    df = pd.DataFrame(data)

    # Mock os.access to simulate permission error
    def mock_access(path, mode):
        return False

    monkeypatch.setattr("os.access", mock_access)

    # Expect a PermissionError
    with pytest.raises(PermissionError, match="No write permissions"):
        transform_and_store_data(df, str(db_path))

 