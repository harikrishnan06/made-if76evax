import os
import toml
import logging
from data_process.fetch_data import fetch_data_from_url
import pandas as pd
import data_transform.ev_sales_data as esd
import data_transform.trasform_gas_data as tgd
from data_transform.pre_process import fetch_and_process_data
from analytics.basic_analysis import plot_separate_graphs_with_normalization_and_save

# Configure Logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def load_config(config_path='project/config.toml'):
    try:
        config = toml.load(config_path)
        return config
    except Exception as e:
        logger.error(f"Error loading config file: {e}")
        raise

def fetch_and_log(url, save_to):
    try:
        fetch_data_from_url(url, save_to)
        logger.info(f"Data fetched successfully from {url} to {save_to}.")
    except Exception as e:
        logger.error(f"Error fetching data from {url}: {e}")
        raise

def extract_process_gas_data(file_path, db_path):
    try:
        df = pd.read_excel(file_path, sheet_name='Data 1', skiprows=2)
        tgd.transform_and_store_data(df, db_path)
        logger.info(f"Gasoline data transformed and stored successfully in {db_path}.")
    except Exception as e:
        logger.error(f"Error processing gasoline data from {file_path}: {e}")
        raise

def extract_process_ev_data(file_path, db_path):
    try:
        logger.info("Starting EV data preprocessing.")
        esd.fetch_and_preprocess_ev_sales(file_path, db_path)
        logger.info(f"EV data transformed and stored successfully in {db_path}.")
    except Exception as e:
        logger.error(f"Error processing EV data from {file_path}: {e}")
        raise

def get_absolute_path(base_dir, relative_path):
    return os.path.abspath(os.path.join(base_dir, relative_path))

def pre_process_data_for_analysis(from_yr, to_yr, gas_db_path, ev_db_path, gas_output_csv_path, ev_output_csv_path, merged_output_csv_path, log_file, sep_log_file, merged_db_path):
    try:
        fetch_and_process_data(from_yr, to_yr, gas_db_path, ev_db_path, gas_output_csv_path, ev_output_csv_path, merged_output_csv_path, log_file, merged_db_path)
        logger.info(f"Preprocessing data for analysis completed successfully.")
    except Exception as e:
        logger.error(f"Error in preprocessing data for analysis: {e}")
        raise

def basic_analysis(db_path, table_name, output_dir):
    try:
        plot_separate_graphs_with_normalization_and_save(db_path, table_name, volume_scale_factor=1000, output_dir=output_dir)
        logger.info(f"Basic analysis completed. Outputs saved to {output_dir}.")
    except Exception as e:
        logger.error(f"Error in performing basic analysis: {e}")
        raise

def pipeline():
    try:
        # Load configuration
        config = load_config()
        data_dir = get_absolute_path(os.path.dirname(__file__), config['settings']['data_dir'])

        # Fetch data
        gas_data_save_to = os.path.join(data_dir, config['settings']['gas_data_file'])
        fetch_and_log(config['settings']['gas_data_url'], gas_data_save_to)

        ev_sales_save_to = os.path.join(data_dir, config['settings']['ev_sales_data_file'])
        fetch_and_log(config['settings']['ev_sales_data_url'], ev_sales_save_to)

        # Process gas and EV data
        gas_db_path = os.path.join(data_dir, config['settings']['gas_db_file'])
        extract_process_gas_data(gas_data_save_to, gas_db_path)

        ev_db_path = os.path.join(data_dir, config['settings']['ev_sales_db_file'])
        extract_process_ev_data(ev_sales_save_to, ev_db_path)

        # Preprocess and merge data
        gas_output_csv_path = os.path.join(data_dir, config['settings']['gas_output_csv_file'])
        ev_output_csv_path = os.path.join(data_dir, config['settings']['ev_output_csv_file'])
        merged_output_csv_path = os.path.join(data_dir, config['settings']['merged_output_csv_file'])
        log_file = os.path.join(data_dir, config['settings']['log_file'])
        sep_log_file = os.path.join(data_dir, config['settings']['sep_log_file'])
        merged_db_path = os.path.join(data_dir, config['settings']['merged_db_file'])

        pre_process_data_for_analysis('2010', '2023', gas_db_path, ev_db_path, gas_output_csv_path, ev_output_csv_path, merged_output_csv_path, log_file, sep_log_file, merged_db_path)

        # Perform basic analysis
        basic_analysis(merged_db_path, "merged_data", data_dir)

        logger.info("Pipeline executed successfully.")
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")

def main():
    pipeline()

if __name__ == "__main__":
    main()
