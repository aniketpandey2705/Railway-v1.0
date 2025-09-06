import pandas as pd

def assign_priority(row):
    """
    Assigns a priority level to a train based on its type.
    A lower number means a higher priority.
    """
    train_name = str(row['Train_Name']).upper()
    train_no = str(row['Train_No'])

    # Priority 1: Premium Trains (Highest)
    premium_keywords = ['RAJDHANI', 'SHATABDI', 'DURONTO', 'VANDE BHARAT', 'TEJAS', 'GATIMAAN']
    if any(keyword in train_name for keyword in premium_keywords):
        return 1

    # Priority 2: Superfast Trains (SF)
    if 'SF' in train_name or train_no.startswith(('12', '20', '22')):
        return 2

    # Priority 3: Mail/Express Trains
    if 'MAIL' in train_name or 'EXPRESS' in train_name or 'EXP' in train_name:
        return 3

    # Priority 4: Special Trains (SPL)
    if 'SPL' in train_name or train_no.startswith(('0', '7', '1667')): # Adding specific special train numbers from data
        return 4

    # Priority 5: Default for any other train type (e.g., Passenger)
    return 5

def prioritize_data(input_filepath, output_filepath):
    """
    Reads the cleaned train data, adds a priority column, and saves the result.
    """
    print(f"Reading data from '{input_filepath}' to add priorities...")
    try:
        df = pd.read_csv(input_filepath)
    except FileNotFoundError:
        print(f"Error: The file '{input_filepath}' was not found. Please run the cleaning script first.")
        return

    # Apply the priority assignment function to each row
    df['Priority'] = df.apply(assign_priority, axis=1)

    # Reorder columns to place Priority after Train_Name for readability
    cols = ['Train_No', 'Train_Name', 'Priority', 'From_Station', 'To_Station', 'Scheduled_Arrival', 'Scheduled_Departure']
    df = df[cols]

    # Save the updated DataFrame to a new CSV file
    df.to_csv(output_filepath, index=False)
    print(f"Successfully added priority column. Data saved to '{output_filepath}'.")

if __name__ == '__main__':
    cleaned_file = 'etrain_cleaned.csv'
    prioritized_file = 'etrain_with_priority.csv'
    prioritize_data(cleaned_file, prioritized_file)