import pandas as pd
import json

def convert_csv_to_json(input_filepath, output_filepath):
    """
    Converts the final prioritized CSV data into a clean JSON array file.
    """
    print(f"Reading data from '{input_filepath}' to convert to JSON...")
    try:
        # Read the final CSV data
        df = pd.read_csv(input_filepath)
        
        # Replace pandas' NaN (Not a Number) with None so it becomes 'null' in JSON
        df = df.where(pd.notnull(df), None)
        
        # Convert the DataFrame to a list of dictionaries (records orientation)
        json_result = df.to_dict(orient='records')
        
        # Save the JSON data to a file with pretty printing for readability
        with open(output_filepath, 'w') as f:
            json.dump(json_result, f, indent=4)
        
        print(f"Conversion complete. JSON data saved to '{output_filepath}'.")
        
    except FileNotFoundError:
        print(f"Error: The file '{input_filepath}' was not found. Please run the priority script first.")
        return

if __name__ == '__main__':
    prioritized_file = 'etrain_with_priority.csv'
    json_file = 'etrain_final.json'
    convert_csv_to_json(prioritized_file, json_file)

