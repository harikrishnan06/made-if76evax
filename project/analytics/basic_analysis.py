import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
def plot_and_save_graph(data, x_col, y_col, y_label, title, output_path, color, scale_factor=None):
    """
    Plots a graph for the given data and saves it as a PNG file.

    Parameters:
    - data: DataFrame containing the data to plot.
    - x_col: Column name for the x-axis.
    - y_col: Column name for the y-axis.
    - y_label: Label for the y-axis.
    - title: Title of the plot.
    - output_path: Path to save the PNG image.
    - color: Color of the plot line.
    - scale_factor: Factor to scale the y-axis values, if needed.
    """
    plt.figure(figsize=(12, 6))
    if scale_factor:
        data[y_col] = data[y_col] / scale_factor
        y_label = f'{y_label} (scaled by {scale_factor})'
    plt.plot(data[x_col], data[y_col], color=color, label=y_label)
    plt.xlabel('Timestamp')
    plt.ylabel(y_label)
    plt.title(title)
    plt.grid(visible=True, linestyle='--', linewidth=0.5)
    plt.legend()
    plt.savefig(output_path)
    plt.close()
    print(f"Saved plot to {output_path}.")

def plot_separate_graphs_with_normalization_and_save(db_path, table_name, volume_scale_factor=1000, output_dir='./'):
    """
    Plots and saves separate graphs for price and normalized volume from the merged data.

    Parameters:
    - db_path: Path to the SQLite database file.
    - table_name: Name of the table containing merged data.
    - volume_scale_factor: Factor to scale down the volume for better visualization.
    - output_dir: Directory to save the PNG images.
    """
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(db_path)
        
        # Load the merged data into a DataFrame
        query = f"SELECT * FROM {table_name}"
        merged_df = pd.read_sql_query(query, conn)
        
        # Ensure the timestamp column is datetime
        merged_df['timestamp'] = pd.to_datetime(merged_df['timestamp'])
        
        # Close the connection
        conn.close()
        
        # Plot and save Price graph
        plot_and_save_graph(
            data=merged_df,
            x_col='timestamp',
            y_col='price',
            y_label='Price',
            title='Gasoline Prices Over Time',
            output_path=f"{output_dir}/gasoline_prices.png",
            color='tab:blue'
        )

        # Plot and save Normalized Volume graph
        plot_and_save_graph(
            data=merged_df,
            x_col='timestamp',
            y_col='volume',
            y_label='Volume',
            title='Normalized EV Volume Over Time',
            output_path=f"{output_dir}/normalized_ev_volume.png",
            color='tab:orange',
            scale_factor=volume_scale_factor
        )
        
    except Exception as e:
        print(f"Error plotting and saving graphs: {e}")



 