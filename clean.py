import pandas as pd
import numpy as np

def clean_raw_data(input_filepath, output_filepath):
    """
    Cleans the raw scraped train data from the provided CSV file.
    This version is tailored for the station-to-station matrix format.
    """
    print(f"Starting data cleaning for '{input_filepath}'...")

    # Define headers based on the observed structure of the raw file
    headers = [
        'From_Station', 'To_Station', 'Scheduled_Arrival', 'Scheduled_Departure', 'Halt_Time',
        'c1', 'c2', 'c3', 'c4', 'c5', 'c6',
        'train_no_1', 'href_1', 'train_name_1',
        'train_no_2', 'train_name_2',
        'train_no_3', 'href_3', 'train_name_3',
        'train_no_4', 'train_name_4'
    ]

    try:
        # Load the CSV, skipping the original header and applying new ones
        df = pd.read_csv(input_filepath, skiprows=1, names=headers, on_bad_lines='skip')
    except FileNotFoundError:
        print(f"Error: The file '{input_filepath}' was not found.")
        return

    # Consolidate the two possible sets of train number and name columns
    df['Train_No'] = df['train_no_1'].fillna(df['train_no_2'])
    df['Train_Name'] = df['train_name_1'].fillna(df['train_name_2'])

    # Forward fill the train number and name for consecutive rows
    df['Train_No'] = df['Train_No'].ffill()
    df['Train_Name'] = df['Train_Name'].ffill()

    # Keep only the columns we need for the final output
    df_clean = df[['Train_No', 'Train_Name', 'From_Station', 'To_Station', 'Scheduled_Arrival', 'Scheduled_Departure']].copy()

    # Clean and standardize data within the columns
    placeholders = ['--', '-', 'RT', 'Y', 'X']
    for col in df_clean.columns:
        df_clean[col] = df_clean[col].replace(placeholders, np.nan)
        if df_clean[col].dtype == 'object':
            df_clean[col] = df_clean[col].str.strip()

    # Drop rows where essential information like Train_No or Train_Name is missing
    # df_clean.dropna(subset=['Train_No', 'Train_Name'], inplace=True)

    # Ensure train numbers are integers
    df_clean['Train_No'] = pd.to_numeric(df_clean['Train_No'], errors='coerce').astype('Int64')
    # df_clean.dropna(subset=['Train_No'], inplace=True)

    # Save the clean data to a new CSV file
    df_clean.to_csv(output_filepath, index=False)
    print(f"Cleaning complete. Cleaned data saved to '{output_filepath}'.")

if __name__ == '__main__':
    raw_file = 'DATA/etrain1.csv'
    cleaned_file = 'DATA/etrain_cleaned.csv'
    clean_raw_data(raw_file, cleaned_file)
