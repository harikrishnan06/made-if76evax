import toml
from data_process.fetch_data import fetch_data

def load_config(config_path='project/config.toml'):
    try:
        config = toml.load(config_path)
        return config
    except Exception as e:
        print(f"Error loading config file: {e}")
        raise

def pipeline():
    try:
        config = load_config()
        url = config['dataset']['gas_data_url']
        fetch_data(url)
        print("Data fetching and processing completed successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    pipeline()

if __name__ == "__main__":
    main()