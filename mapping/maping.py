import pandas as pd
import json

def create_section_schedule(bhopal_filepath, itarsi_filepath, network_model):
    """
    Reads clean JSON data, finds common trains, synthesizes intermediate stops,
    and builds a detailed event path for each train suitable for simulation.
    """
    print("Step 1: Loading clean data from station files...")
    try:
        with open(bhopal_filepath, 'r') as f:
            bhopal_data = json.load(f)
        with open(itarsi_filepath, 'r') as f:
            itarsi_data = json.load(f)
        print(f"  - Loaded {len(bhopal_data)} records from '{bhopal_filepath}'.")
        print(f"  - Loaded {len(itarsi_data)} records from '{itarsi_filepath}'.")
    except FileNotFoundError as e:
        print(f"Error: Could not find a required data file. {e}")
        return []

    bhopal_df = pd.DataFrame(bhopal_data)
    itarsi_df = pd.DataFrame(itarsi_data)
    section_schedule = []

    # --- Data Type Standardization ---
    print("\nStep 1.5: Standardizing Train_No data type...")
    for df in [bhopal_df, itarsi_df]:
        df['Train_No'] = pd.to_numeric(df['Train_No'], errors='coerce')
        df.dropna(subset=['Train_No'], inplace=True)
        df['Train_No'] = df['Train_No'].astype(int)
    print("  - Data types standardized.")

    # --- Process trains traveling DOWN (from Bhopal to Itarsi) ---
    print("\nStep 2: Finding common trains for Bhopal -> Itarsi (DOWN)...")
    
    # Corrected Logic: Merge on Train_No FIRST to find all common trains.
    merged_down_trains = pd.merge(bhopal_df, itarsi_df, on='Train_No', suffixes=('_BPL', '_ET'))
    print(f"  - Found {len(merged_down_trains)} common train numbers.")
    
    # THEN, filter for valid journeys (must have departure from BPL and arrival at ET)
    valid_down_journeys = merged_down_trains.dropna(subset=['Scheduled_Departure_BPL', 'Scheduled_Arrival_ET'])
    print(f"  - Of those, {len(valid_down_journeys)} have a valid schedule for a DOWN journey.")

    for _, row in valid_down_journeys.iterrows():
        bpl_departure_time = pd.to_datetime(row['Scheduled_Departure_BPL'], format='%H:%M', errors='coerce')
        et_arrival_time = pd.to_datetime(row['Scheduled_Arrival_ET'], format='%H:%M', errors='coerce')

        if pd.isna(bpl_departure_time) or pd.isna(et_arrival_time):
            continue

        total_travel_minutes = (et_arrival_time - bpl_departure_time).total_seconds() / 60
        if total_travel_minutes < 0:
            total_travel_minutes += 1440 

        if total_travel_minutes <= 10:
            continue
            
        bpl_to_hbd_dist, total_dist = 74, 92
        hbd_arrival_minutes = total_travel_minutes * (bpl_to_hbd_dist / total_dist)
        hbd_arrival_time = bpl_departure_time + pd.Timedelta(minutes=hbd_arrival_minutes)
        hbd_departure_time = hbd_arrival_time + pd.Timedelta(minutes=2)

        path = [
            {"type": "DEPARTURE", "station_id": "BPL", "time": bpl_departure_time.strftime('%H:%M')},
            {"type": "TRAVERSE", "segment_id": "SEG_BPL_HBD"},
            {"type": "ARRIVAL", "station_id": "HBD", "time": hbd_arrival_time.strftime('%H:%M')},
            {"type": "HALT", "station_id": "HBD", "duration_mins": 2},
            {"type": "DEPARTURE", "station_id": "HBD", "time": hbd_departure_time.strftime('%H:%M')},
            {"type": "TRAVERSE", "segment_id": "SEG_HBD_ET"},
            {"type": "ARRIVAL", "station_id": "ET", "time": et_arrival_time.strftime('%H:%M')}
        ]

        train_entry = {"Train_No": int(row['Train_No']), "Train_Name": row['Train_Name_BPL'], "direction": "DOWN", "path": path}
        section_schedule.append(train_entry)

    # --- Process trains traveling UP (from Itarsi to Bhopal) ---
    print("\nStep 3: Finding common trains for Itarsi -> Bhopal (UP)...")
    merged_up_trains = pd.merge(itarsi_df, bhopal_df, on='Train_No', suffixes=('_ET', '_BPL'))
    print(f"  - Found {len(merged_up_trains)} common train numbers.")
    
    valid_up_journeys = merged_up_trains.dropna(subset=['Scheduled_Departure_ET', 'Scheduled_Arrival_BPL'])
    print(f"  - Of those, {len(valid_up_journeys)} have a valid schedule for an UP journey.")

    for _, row in valid_up_journeys.iterrows():
        et_departure_time = pd.to_datetime(row['Scheduled_Departure_ET'], format='%H:%M', errors='coerce')
        bpl_arrival_time = pd.to_datetime(row['Scheduled_Arrival_BPL'], format='%H:%M', errors='coerce')

        if pd.isna(et_departure_time) or pd.isna(bpl_arrival_time):
            continue

        total_travel_minutes = (bpl_arrival_time - et_departure_time).total_seconds() / 60
        if total_travel_minutes < 0:
            total_travel_minutes += 1440
        
        if total_travel_minutes <= 10:
            continue

        et_to_hbd_dist, total_dist = 18, 92
        hbd_arrival_minutes = total_travel_minutes * (et_to_hbd_dist / total_dist)
        hbd_arrival_time = et_departure_time + pd.Timedelta(minutes=hbd_arrival_minutes)
        hbd_departure_time = hbd_arrival_time + pd.Timedelta(minutes=2)

        path = [
            {"type": "DEPARTURE", "station_id": "ET", "time": et_departure_time.strftime('%H:%M')},
            {"type": "TRAVERSE", "segment_id": "SEG_HBD_ET"},
            {"type": "ARRIVAL", "station_id": "HBD", "time": hbd_arrival_time.strftime('%H:%M')},
            {"type": "HALT", "station_id": "HBD", "duration_mins": 2},
            {"type": "DEPARTURE", "station_id": "HBD", "time": hbd_departure_time.strftime('%H:%M')},
            {"type": "TRAVERSE", "segment_id": "SEG_BPL_HBD"},
            {"type": "ARRIVAL", "station_id": "BPL", "time": bpl_arrival_time.strftime('%H:%M')}
        ]

        train_entry = {"Train_No": int(row['Train_No']), "Train_Name": row['Train_Name_ET'], "direction": "UP", "path": path}
        section_schedule.append(train_entry)
        
    return section_schedule

def main():
    # Define the physical network model for the section
    network_model = {
      "section_name": "Bhopal (BPL) to Itarsi (ET) - Major Stations",
      "stations": [
        {"id": "BPL", "name": "Bhopal Junction", "km_from_start": 0},
        {"id": "HBD", "name": "Hoshangabad", "km_from_start": 74},
        {"id": "ET", "name": "Itarsi Junction", "km_from_start": 92}
      ],
      "segments": [
        {"id": "SEG_BPL_HBD", "from": "BPL", "to": "HBD", "distance_km": 74, "type": "double"},
        {"id": "SEG_HBD_ET", "from": "HBD", "to": "ET", "distance_km": 18, "type": "double"}
      ]
    }
    
    # Create the full section schedule by processing the two station files
    synthesized_schedule = create_section_schedule(
        'bhopal_data.json', 
        'itarsri_data.json', # Corrected filename to match user's upload
        network_model
    )

    # Combine into the final simulation data package
    final_simulation_data = {
        "network_model": network_model,
        "train_schedule": synthesized_schedule
    }

    # Save the final mapped data to a new JSON file
    output_filename = 'bpl_et_simulation_with_paths.json'
    with open(output_filename, 'w') as f:
        json.dump(final_simulation_data, f, indent=4)
        
    print(f"\nStep 4: Mapping complete! The synthesized simulation data with event paths has been saved to '{output_filename}'.")
    if not synthesized_schedule:
        print("\nWARNING: The final train schedule is still empty. Please check your JSON files for matching Train_No values with valid departure/arrival times.")


if __name__ == '__main__':
    main()

