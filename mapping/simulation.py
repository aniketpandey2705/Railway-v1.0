import json
from datetime import datetime, timedelta

class TrainSimulator:
    """
    A simulation engine for train movements within a defined railway section.
    """
    def __init__(self, network_model, train_schedule):
        self.network = network_model
        self.schedule = train_schedule
        self.current_time = None
        self.train_states = {}
        self.segment_occupancy = {segment['id']: None for segment in self.network['segments']}

    def initialize(self, start_time_str):
        """Sets up the initial state of the simulation."""
        self.current_time = datetime.strptime(start_time_str, '%H:%M')
        for train in self.schedule:
            train_no = train['Train_No']
            first_event = train['path'][0]
            if first_event['type'] == 'DEPARTURE':
                self.train_states[train_no] = {
                    "location": first_event['station_id'],
                    "status": "SCHEDULED",
                    "path_index": 0
                }
        print(f"--- Simulation Initialized. Start Time: {start_time_str} ---")

    def run(self, duration_mins):
        """Runs the simulation for a specified duration."""
        end_time = self.current_time + timedelta(minutes=duration_mins)
        print(f"--- Running Simulation for {duration_mins} minutes until {end_time.strftime('%H:%M')} ---")

        while self.current_time < end_time:
            print(f"\n[Time: {self.current_time.strftime('%H:%M')}]")
            self._process_departures()
            self._process_arrivals()
            self.current_time += timedelta(minutes=1)
        
        print("\n--- Simulation Complete ---")

    def _process_departures(self):
        """Processes scheduled train departures for the current time."""
        for train in self.schedule:
            train_no = train['Train_No']
            state = self.train_states.get(train_no)
            if not state or state['status'] != "SCHEDULED": continue

            current_event = train['path'][state['path_index']]
            if current_event['type'] == 'DEPARTURE' and datetime.strptime(current_event['time'], '%H:%M').time() == self.current_time.time():
                next_event = train['path'][state['path_index'] + 1]
                segment_id = next_event['segment_id']
                
                if self.segment_occupancy[segment_id] is None:
                    self.segment_occupancy[segment_id] = train_no
                    state.update({"status": "EN_ROUTE", "location": segment_id, "path_index": state['path_index'] + 1})
                    print(f"  EVENT: Train {train_no} ({train['Train_Name']}) DEPARTED from {current_event['station_id']} onto {segment_id}.")
                else:
                    occupying_train = self.segment_occupancy[segment_id]
                    print(f"  CONFLICT: Train {train_no} cannot depart. Segment {segment_id} is occupied by Train {occupying_train}.")

    def _process_arrivals(self):
        """Processes train arrivals for the current time."""
        for train in self.schedule:
            train_no = train['Train_No']
            state = self.train_states.get(train_no)
            if not state or state['status'] != "EN_ROUTE": continue

            arrival_event = train['path'][state['path_index'] + 1]
            if arrival_event['type'] == 'ARRIVAL' and datetime.strptime(arrival_event['time'], '%H:%M').time() == self.current_time.time():
                segment_to_free = state['location']
                self.segment_occupancy[segment_to_free] = None
                
                # A real sim would handle the halt, for now we just mark as arrived at station
                state.update({"status": "ARRIVED", "location": arrival_event['station_id'], "path_index": state['path_index'] + 1})
                print(f"  EVENT: Train {train_no} ({train['Train_Name']}) ARRIVED at {arrival_event['station_id']}, freeing segment {segment_to_free}.")


def run_simulation(simulation_filepath, start_time, duration):
    """Loads a simulation file and runs the simulation."""
    print("\n--- STEP 2: RUNNING SIMULATION ---")
    try:
        with open(simulation_filepath, 'r') as f:
            data = json.load(f)
        network = data.get('network_model')
        schedule = data.get('train_schedule')
        if not network or schedule is None:
            print("Error: Simulation file is missing 'network_model' or 'train_schedule'.")
            return
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading simulation file: {e}")
        return

    if schedule:
        sim = TrainSimulator(network, schedule)
        sim.initialize(start_time_str=start_time)
        sim.run(duration_mins=duration)
    else:
        print("Could not run simulation because the train schedule is empty.")


if __name__ == '__main__':
    # This file is created by the mapping script
    simulation_file = 'bpl_et_common_trains.json'
    
    # You can change these parameters to simulate different time windows
    SIMULATION_START_TIME = "18:00"
    SIMULATION_DURATION_MINUTES = 120
    
    run_simulation(simulation_file, SIMULATION_START_TIME, SIMULATION_DURATION_MINUTES)
