import json
import random
import pandas as pd

def parse_time_and_add_delay(time_str, delay_minutes):
    """
    Adds delay to a time string (HH:MM) and returns a new time string.
    """
    if not time_str or pd.isna(time_str):
        return None
    try:
        time_obj = pd.to_datetime(time_str, format='%H:%M')
        delayed_time = time_obj + pd.Timedelta(minutes=delay_minutes)
        return delayed_time.strftime('%H:%M')
    except (ValueError, TypeError):
        return time_str # Return original if format is incorrect

def inject_disruptions(input_filepath, output_filepath):
    """
    Reads a clean JSON schedule and injects random delays and disruptions.
    """
    print(f"Reading clean schedule from '{input_filepath}'...")
    try:
        with open(input_filepath, 'r') as f:
            schedule = json.load(f)
    except FileNotFoundError:
        print(f"Error: The file '{input_filepath}' was not found. Please run the previous scripts first.")
        return

    # Define possible real-world disruptions
    disruption_types = [
        {"type": "Engine Failure", "min_delay": 30, "max_delay": 90},
        {"type": "Signal Fault Ahead", "min_delay": 10, "max_delay": 25},
        {"type": "Track Maintenance", "min_delay": 20, "max_delay": 45},
        {"type": "Platform Congestion", "min_delay": 5, "max_delay": 15},
    ]

    disrupted_schedule = []
    
    # Set the probability of a train having a disruption (e.g., 25%)
    PROBABILITY_OF_DISRUPTION = 0.25

    print("Injecting random real-world disruptions...")
    for train in schedule:
        if random.random() < PROBABILITY_OF_DISRUPTION:
            # This train will have a disruption
            disruption = random.choice(disruption_types)
            delay = random.randint(disruption['min_delay'], disruption['max_delay'])

            # Add new keys to the train's data
            train['Disruption_Type'] = disruption['type']
            train['Delay_Mins'] = delay
            
            # Calculate new, delayed arrival and departure times
            train['Actual_Arrival'] = parse_time_and_add_delay(train['Scheduled_Arrival'], delay)
            train['Actual_Departure'] = parse_time_and_add_delay(train['Scheduled_Departure'], delay)
            
            print(f"  - DISRUPTION: Train {train['Train_No']} ({train['Train_Name']}) hit a '{disruption['type']}' causing a {delay} min delay.")
        else:
            # This train is on time
            train['Disruption_Type'] = None
            train['Delay_Mins'] = 0
            train['Actual_Arrival'] = train['Scheduled_Arrival']
            train['Actual_Departure'] = train['Scheduled_Departure']

        disrupted_schedule.append(train)

    # Save the new, disrupted schedule to a file
    with open(output_filepath, 'w') as f:
        json.dump(disrupted_schedule, f, indent=4)

    print(f"\nDisruption injection complete. New schedule saved to '{output_filepath}'.")

if __name__ == '__main__':
    clean_schedule_file = 'etrain_final.json'
    disrupted_schedule_file = 'etrain_with_disruptions.json'
    inject_disruptions(clean_schedule_file, disrupted_schedule_file)