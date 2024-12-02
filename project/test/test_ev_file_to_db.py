import os
import pytest
import pandas as pd
import sqlite3
import sys

# Add parent directory to sys.path to import the function
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from data_transform.ev_sales_data import fetch_and_preprocess_ev_sales, preprocess_ev_sales_data, save_to_sqlite

@pytest.fixture
def setup_environment(tmp_path):
    """Setup temporary paths for testing."""
    csv_path = tmp_path / "test_ev_sales.csv"
    db_path = tmp_path / "test_ev_sales.sqlite"
    return csv_path, db_path

@pytest.fixture
def generate_valid_dataset(setup_environment):
    """Generate a valid synthetic EV sales dataset."""
    csv_path, _ = setup_environment
    data = {
        "Registration Valid Date": pd.date_range(start="2023-01-01", periods=5, freq="M"),
        "Vehicle Name": [f"Vehicle {i}" for i in range(5)]
    }
    df = pd.DataFrame(data)
    df.to_csv(csv_path, index=False)
    return csv_path

def test_fetch_and_preprocess_success(generate_valid_dataset, setup_environment):
    """Test the full pipeline with a valid dataset."""
    csv_path, db_path = setup_environment
    csv_path = generate_valid_dataset

    # Execute the full pipeline
    fetch_and_preprocess_ev_sales(str(csv_path), str(db_path))

    # Verify the SQLite database content
    conn = sqlite3.connect(db_path)
    result_df = pd.read_sql_query("SELECT * FROM ev_sales", conn)
    conn.close()

    assert not result_df.empty, "Database should not be empty after processing."
    assert "registration_date" in result_df.columns, "Column 'registration_date' missing in database."
    assert "vehicle_name" in result_df.columns, "Column 'vehicle_name' missing in database."
    assert len(result_df) == 5, "Expected 5 rows in the database."

def test_missing_columns_exception(setup_environment):
    """Test preprocessing with missing required columns."""
    csv_path, db_path = setup_environment

    # Generate a dataset with missing columns
    data = {
        "Invalid Column 1": [1, 2, 3],
        "Invalid Column 2": ["A", "B", "C"]
    }
    df = pd.DataFrame(data)
    df.to_csv(csv_path, index=False)

    # Expect a ValueError for missing required columns
    with pytest.raises(ValueError, match="The expected columns .* are missing from the CSV file"):
        fetch_and_preprocess_ev_sales(str(csv_path), str(db_path))

def test_invalid_dates_dropna(setup_environment):
    """Test that rows with invalid dates are dropped."""
    csv_path, db_path = setup_environment

    # Generate a dataset with invalid dates
    data = {
        "Registration Valid Date": ["2023-01-01", "InvalidDate", None, "2023-03-01", "AnotherInvalidDate"],
        "Vehicle Name": ["Car A", "Car B", "Car C", "Car D", "Car E"]
    }
    df = pd.DataFrame(data)
    df.to_csv(csv_path, index=False)

    # Execute the full pipeline
    fetch_and_preprocess_ev_sales(str(csv_path), str(db_path))

    # Verify the SQLite database content
    conn = sqlite3.connect(db_path)
    result_df = pd.read_sql_query("SELECT * FROM ev_sales", conn)
    conn.close()

    # Assert that only valid rows remain
    assert len(result_df) == 2, "Expected 2 valid rows in the database."
    assert "registration_date" in result_df.columns, "Column 'registration_date' missing in database."
    assert "vehicle_name" in result_df.columns, "Column 'vehicle_name' missing in database."

def test_save_to_nonexistent_directory(setup_environment):
    """Test saving data to a non-existent directory."""
    csv_path, _ = setup_environment
    invalid_db_path = os.path.join("nonexistent_dir", "test_ev_sales.sqlite")

    # Generate a valid dataset
    data = {
        "Registration Valid Date": pd.date_range(start="2023-01-01", periods=5, freq="M"),
        "Vehicle Name": [f"Vehicle {i}" for i in range(5)]
    }
    df = pd.DataFrame(data)
    df.to_csv(csv_path, index=False)

    # Execute the pipeline with a non-existent directory
    fetch_and_preprocess_ev_sales(str(csv_path), invalid_db_path)

    # Verify the database is created and data is saved
    conn = sqlite3.connect(invalid_db_path)
    result_df = pd.read_sql_query("SELECT * FROM ev_sales", conn)
    conn.close()

    assert len(result_df) == 5, "Expected 5 rows in the database."
