import pandas as pd

def clean_new_data(input_filepath, output_filepath):
    """
    Cleans the raw scraped train data from etrain (2).csv, which has a
    row-spanning structure.
    """
    print(f"Starting data cleaning for '{input_filepath}'...")

    # Define headers based on the observed structure of the new raw file.
    # We give names to all potential columns to handle ragged rows.
    headers = [
        'train_no_1', 'href_1', 'train_name_1', 'from_station', 'to_station',
        'scheduled_arrival', 'scheduled_departure', 'halt_time', 'c1', 'c2',
        'c3', 'c4', 'c5', 'c6', 'c7', 'train_no_2', 'href_2', 'train_name_2',
        'c8', 'c9'
    ]

    try:
        # Load the CSV, skipping the original header and applying new ones.
        df = pd.read_csv(input_filepath, header=0, names=headers, on_bad_lines='skip')
    except FileNotFoundError:
        print(f"Error: The file '{input_filepath}' was not found.")
        return

    # Consolidate the two possible sets of train number and name columns.
    df['Train_No'] = df['train_no_1'].fillna(df['train_no_2'])
    df['Train_Name'] = df['train_name_1'].fillna(df['train_name_2'])

    # The key step: Forward-fill the train information to subsequent rows.
    # This assigns the train info to all its corresponding station stops.
    df['Train_No'] = df['Train_No'].ffill()
    df['Train_Name'] = df['Train_Name'].ffill()

    # Select and rename the final, relevant columns.
    df_clean = df[[
        'Train_No', 'Train_Name', 'from_station', 'to_station',
        'scheduled_arrival', 'scheduled_departure'
    ]].copy()
    df_clean.rename(columns={
        'from_station': 'From_Station',
        'to_station': 'To_Station',
        'scheduled_arrival': 'Scheduled_Arrival',
        'scheduled_departure': 'Scheduled_Departure'
    }, inplace=True)

    # Clean data: remove placeholders and strip whitespace.
    placeholders = ['--', '-', 'RT', 'Y', 'X']
    for col in df_clean.columns:
        df_clean[col] = df_clean[col].replace(placeholders, pd.NA)
        if df_clean[col].dtype == 'object':
            df_clean[col] = df_clean[col].str.strip()

    # Drop rows that don't have any station information.
    df_clean.dropna(subset=['From_Station', 'To_Station'], how='all', inplace=True)

    # Ensure train numbers are valid integers.
    df_clean['Train_No'] = pd.to_numeric(df_clean['Train_No'], errors='coerce').astype('Int64')
    df_clean.dropna(subset=['Train_No'], inplace=True)

    # Save the clean data to a new CSV file.
    df_clean.to_csv(output_filepath, index=False)
    print(f"Cleaning complete. Cleaned data saved to '{output_filepath}'.")


if __name__ == '__main__':
    raw_file = 'etrain.csv'
    cleaned_file = 'etrain_cleaned.csv'
    clean_new_data(raw_file, cleaned_file)
