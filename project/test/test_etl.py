import os
import pytest
import pandas as pd
import sqlite3
import sys

# Add parent directory to sys.path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from data_transform.pre_process import process_gas_data, process_ev_data, merge_data, save_to_db

@pytest.fixture
def setup_environment(tmp_path):
    """Setup temporary paths for testing."""
    gas_db_path = tmp_path / "gasoline.db"
    ev_db_path = tmp_path / "ev_sales.db"
    log_file = tmp_path / "log_file.txt"
    gas_output_csv = tmp_path / "gas_output.csv"
    ev_output_csv = tmp_path / "ev_output.csv"
    merged_output_csv = tmp_path / "merged_output.csv"
    db_path = tmp_path / "merged.db"
    return {
        "gas_db": gas_db_path,
        "ev_db": ev_db_path,
        "log_file": log_file,
        "gas_csv": gas_output_csv,
        "ev_csv": ev_output_csv,
        "merged_csv": merged_output_csv,
        "db": db_path,
    }

@pytest.fixture
def create_mock_data(setup_environment):
    """Create mock databases for testing."""
    env = setup_environment

    # Gasoline data with inconsistencies
    gas_data = pd.DataFrame({
        "timestamp": [
            "2023-01-01", "2023-01-08", "2023-01-15",  # Consistent
            "2023-01-25",  # Inconsistent (10-day gap)
            "2023-02-01",  # Inconsistent
            "2023-02-08"   # Consistent
        ],
        "price": [3.5, 3.6, 3.7, None, 4.0, 3.9],
    })
    conn = sqlite3.connect(env["gas_db"])
    gas_data.to_sql("gasoline_prices", conn, if_exists="replace", index=False)
    conn.close()

    # EV sales data with some missing months
    ev_data = pd.DataFrame({
        "registration_date": [
            "2023-01-01", "2023-02-01", "2023-04-01"  # Missing March
        ],
        "volume": [100, 200, 150]
    })
    conn = sqlite3.connect(env["ev_db"])
    ev_data.to_sql("ev_sales", conn, if_exists="replace", index=False)
    conn.close()

    return env

def test_process_gas_correct_data(setup_environment):
    """Test processing gas data with correct weekly intervals."""
    env = setup_environment

    # Create consistent gas data
    gas_data = pd.DataFrame({
        "timestamp": pd.date_range(start="2023-01-01", periods=6, freq="W"),
        "price": [3.5, 3.6, 3.7, 3.8, 4.0, 4.1],
    })

    processed_gas = process_gas_data(gas_data, str(env["log_file"]))

    # Verify results
    assert len(processed_gas) == 2, "Expected 2 months in the processed gas data."
    assert "timestamp" in processed_gas.columns, "Processed gas data missing 'timestamp' column."
    assert "price" in processed_gas.columns, "Processed gas data missing 'price' column."

    # Verify log file is empty
    with open(env["log_file"], "r") as log:
        logs = log.read()
    assert logs == "", "Log file should be empty for correct weekly intervals."

def test_process_gas_inconsistent_data(create_mock_data):
    """Test processing gas data with weekly inconsistencies."""
    env = create_mock_data

    # Read gas data and process
    conn = sqlite3.connect(env["gas_db"])
    gas_df = pd.read_sql_query("SELECT * FROM gasoline_prices", conn)
    conn.close()

    processed_gas = process_gas_data(gas_df, str(env["log_file"]))

    # Verify results
    assert len(processed_gas) == 2, "Expected 2 months in the processed gas data."
    assert "timestamp" in processed_gas.columns, "Processed gas data missing 'timestamp' column."
    assert "price" in processed_gas.columns, "Processed gas data missing 'price' column."

    # Verify log file contains inconsistencies
    with open(env["log_file"], "r") as log:
        logs = log.readlines()
    assert len(logs) > 0, "Log file should not be empty for inconsistent weekly intervals."
    assert "Inconsistency found" in logs[0], "Expected log entry for weekly inconsistency."

def test_merge_with_missing_months(create_mock_data):
    """Test merging gas and EV data with mismatched months."""
    env = create_mock_data

    # Process gas data
    conn = sqlite3.connect(env["gas_db"])
    gas_df = pd.read_sql_query("SELECT * FROM gasoline_prices", conn)
    conn.close()
    gas_monthly = process_gas_data(gas_df, str(env["log_file"]))

    # Process EV data
    conn = sqlite3.connect(env["ev_db"])
    ev_df = pd.read_sql_query("SELECT * FROM ev_sales", conn)
    conn.close()
    ev_monthly = process_ev_data(ev_df)

    # Merge data
    merged_data = merge_data(gas_monthly, ev_monthly, str(env["merged_csv"]), str(env["log_file"]))

    # Verify merged data
    assert len(merged_data) == 3, "Expected 3 months in the merged data."
    assert "timestamp" in merged_data.columns, "Merged data missing 'timestamp' column."
    assert "price" in merged_data.columns, "Merged data missing 'price' column."
    assert "volume" in merged_data.columns, "Merged data missing 'volume' column."

    # Verify log file contains mismatches
    with open(env["log_file"], "r") as log:
        logs = log.readlines()
    assert any("Missing data" in log for log in logs), "Expected log entries for missing data mismatches."

def test_save_to_database(create_mock_data):
    """Test saving merged data to SQLite database."""
    env = create_mock_data

    # Process gas data
    conn = sqlite3.connect(env["gas_db"])
    gas_df = pd.read_sql_query("SELECT * FROM gasoline_prices", conn)
    conn.close()
    gas_monthly = process_gas_data(gas_df, str(env["log_file"]))

    # Process EV data
    conn = sqlite3.connect(env["ev_db"])
    ev_df = pd.read_sql_query("SELECT * FROM ev_sales", conn)
    conn.close()
    ev_monthly = process_ev_data(ev_df)

    # Merge data
    merged_data = merge_data(gas_monthly, ev_monthly, str(env["merged_csv"]), str(env["log_file"]))

    # Save merged data to SQLite
    save_to_db(merged_data, str(env["db"]))

    # Verify database content
    conn = sqlite3.connect(env["db"])
    result_df = pd.read_sql_query("SELECT * FROM merged_data", conn)
    conn.close()

    assert not result_df.empty, "Database save failed; result is empty."
    assert "timestamp" in result_df.columns, "Saved database missing 'timestamp'."
    assert "price" in result_df.columns, "Saved database missing 'price'."
    assert "volume" in result_df.columns, "Saved database missing 'volume'."
