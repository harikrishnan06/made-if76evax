import pandas as pd
import sqlite3
import logging

# Configure logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def process_gas_data(gas_df, log_file):
    try:
        gas_df['timestamp'] = pd.to_datetime(gas_df['timestamp'])
        gas_df = gas_df.sort_values(by='timestamp')

        # Check weekly consistency and normalize disruptions
        gas_df['week_diff'] = gas_df['timestamp'].diff().dt.days
        inconsistent_rows = gas_df[(gas_df['week_diff'] != 7) & (gas_df['week_diff'].notnull())]

        with open(log_file, 'w') as log:
            for _, row in inconsistent_rows.iterrows():
                log.write(f"Inconsistency found: {row['timestamp']} - {row['price']}\n")

        # Normalize disruptions
        for i in range(1, len(gas_df)):
            if gas_df.iloc[i]['week_diff'] != 7:
                if i + 1 < len(gas_df):
                    gas_df.at[i, 'price'] = (gas_df.iloc[i - 1]['price'] + gas_df.iloc[i + 1]['price']) / 2
                elif i - 1 >= 0:
                    gas_df.at[i, 'price'] = gas_df.iloc[i - 1]['price']

        # Drop helper columns and group by month
        gas_df.drop(columns=['week_diff'], inplace=True)
        gas_df['timestamp'] = gas_df['timestamp'].dt.to_period('M').dt.to_timestamp('M')
        gas_monthly = gas_df.groupby('timestamp')['price'].mean().reset_index()

        logger.info("Processed gasoline data successfully.")
        return gas_monthly
    except Exception as e:
        logger.error(f"Error processing gasoline data: {e}")
        return pd.DataFrame()

def process_ev_data(ev_df):
    try:
        ev_df['timestamp'] = pd.to_datetime(ev_df['registration_date']).dt.to_period('M').dt.to_timestamp('M')
        ev_monthly = ev_df.groupby('timestamp').size().reset_index(name='volume')
        logger.info("Processed EV data successfully.")
        return ev_monthly
    except Exception as e:
        logger.error(f"Error processing EV data: {e}")
        return pd.DataFrame()

def merge_data(gas_monthly, ev_monthly, merged_output_csv_path, log_file):
    try:
        merged_df = pd.merge(gas_monthly, ev_monthly, on='timestamp', how='outer')
        merged_df['price'].fillna('NIL', inplace=True)
        merged_df['volume'].fillna('NIL', inplace=True)

        # Log mismatched rows
        with open(log_file, 'a') as log:
            for _, row in merged_df.iterrows():
                if row['price'] == 'NIL' or row['volume'] == 'NIL':
                    log.write(f"Missing data for {row['timestamp']}: Gas - {row['price']}, EV - {row['volume']}\n")

        # Remove rows where both values are missing
        merged_df = merged_df[(merged_df['price'] != 'NIL') | (merged_df['volume'] != 'NIL')]

        # Save merged data
        merged_df.to_csv(merged_output_csv_path, index=False)
        logger.info(f"Merged data saved to {merged_output_csv_path}.")
        return merged_df
    except Exception as e:
        logger.error(f"Error merging data: {e}")
        return pd.DataFrame()

def save_to_db(merged_df, db_path):
    try:
        validated_df = merged_df[(merged_df['price'] != 'NIL') & (merged_df['volume'] != 'NIL')]

        # Ensure correct data types
        validated_df['timestamp'] = pd.to_datetime(validated_df['timestamp'])
        validated_df['price'] = validated_df['price'].astype(float)
        validated_df['volume'] = validated_df['volume'].astype(int)

        # Save to SQLite database
        conn = sqlite3.connect(db_path)
        validated_df.to_sql('merged_data', conn, if_exists='replace', index=False)
        conn.close()
        logger.info(f"Merged data saved to SQLite database at {db_path}.")
    except Exception as e:
        logger.error(f"Error saving data to SQLite: {e}")

def fetch_and_process_data(from_yr, to_yr, gas_db_path, ev_db_path, gas_output_csv_path, ev_output_csv_path, merged_output_csv_path, log_file, db_path):
    try:
        # Fetch and process gasoline data
        gas_conn = sqlite3.connect(gas_db_path)
        gas_query = f"""
        SELECT timestamp, price FROM gasoline_prices
        WHERE strftime('%Y', timestamp) BETWEEN '{from_yr}' AND '{to_yr}'
        """
        gas_df = pd.read_sql_query(gas_query, gas_conn)
        gas_conn.close()
        gas_monthly = process_gas_data(gas_df, log_file)
        gas_monthly.to_csv(gas_output_csv_path, index=False)
        logger.info(f"Gas data saved to {gas_output_csv_path}.")

        # Fetch and process EV data
        ev_conn = sqlite3.connect(ev_db_path)
        ev_query = f"""
        SELECT registration_date FROM ev_sales
        WHERE strftime('%Y', registration_date) BETWEEN '{from_yr}' AND '{to_yr}'
        """
        ev_df = pd.read_sql_query(ev_query, ev_conn)
        ev_conn.close()
        ev_monthly = process_ev_data(ev_df)
        ev_monthly.to_csv(ev_output_csv_path, index=False)
        logger.info(f"EV data saved to {ev_output_csv_path}.")

        # Merge processed data
        merged_df = merge_data(gas_monthly, ev_monthly, merged_output_csv_path, log_file)

        # Save merged data to SQLite database
        save_to_db(merged_df, db_path)
    except Exception as e:
        logger.error(f"Error fetching and processing data: {e}")

if __name__ == "__main__":
    from_yr = '2010'
    to_yr = '2021'
    gas_db_path = 'C://Users//harik//Desktop//MADE//made-if76evax//data//raw_Gas.database.db'
    ev_db_path = 'C://Users//harik//Desktop//MADE//made-if76evax//data//ev_sales.db'
    gas_output_csv_path = 'C://Users//harik//Desktop//MADE//made-if76evax//data//cropped_gas_data.csv'
    ev_output_csv_path = 'C://Users//harik//Desktop//MADE//made-if76evax//data//cropped_ev_data.csv'
    merged_output_csv_path = 'C://Users//harik//Desktop//MADE//made-if76evax//data//merged_data.csv'
    log_file = 'C://Users//harik//Desktop//MADE//made-if76evax//data//inconsistencies_log.txt'
    db_path = 'C://Users//harik//Desktop//MADE//made-if76evax//data//merged_data.db'

    fetch_and_process_data(from_yr, to_yr, gas_db_path, ev_db_path, gas_output_csv_path, ev_output_csv_path, merged_output_csv_path, log_file, db_path)
