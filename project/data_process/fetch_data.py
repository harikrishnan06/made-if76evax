import os
import requests
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

def download_file(url, local_path):
    """Download a file from a URL and save it to a local path."""
    try:
        response = requests.get(url, timeout=10)  # Add timeout for better error handling
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx, 5xx)
        with open(local_path, 'wb') as file:
            file.write(response.content)
        logging.info(f"File downloaded successfully and saved to {local_path}")
    except requests.exceptions.Timeout:
        logging.error(f"Error: The request to {url} timed out.")
        raise
    except requests.exceptions.ConnectionError:
        logging.error(f"Error: Unable to connect to {url}. Check your network connection.")
        raise
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred while accessing {url}: {http_err}")
        raise
    except requests.exceptions.RequestException as req_err:
        logging.error(f"An error occurred while downloading from {url}: {req_err}")
        raise

def fetch_data_from_url(url, save_to):
    """Fetch data from a URL and save it to a local file."""
    try:
        download_file(url, save_to)
        logging.info("Data fetching and saving to file completed successfully.")
    except requests.exceptions.Timeout:
        logging.error(f"Failed to fetch data due to a timeout error.")
        raise
    except requests.exceptions.ConnectionError:
        logging.error(f"Failed to fetch data due to a connection error.")
        raise
    except requests.exceptions.HTTPError:
        logging.error(f"Failed to fetch data due to an HTTP error.")
        raise
    except requests.exceptions.RequestException:
        logging.error(f"Failed to fetch data due to a general request error.")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise
