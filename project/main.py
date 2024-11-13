from data_process.fetch_data import fetch_data

def pipeline():
    try:
        fetch_data()
        print("Data fetching and processing completed successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    pipeline()

if __name__ == "__main__":
    main()