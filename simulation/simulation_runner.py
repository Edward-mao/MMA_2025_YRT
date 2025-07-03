"""
Main runner for the SimPy bus simulation.
"""
import simpy
import os
import sys
import random
import yaml
import time
from pathlib import Path
from typing import Dict, List, Any, Optional
import shutil  # Added for folder deletion
from datetime import datetime

# Ensure script can find the simulation package
# This needs to be done BEFORE attempting to import from 'simulation'
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from simulation.passenger import Passenger
from simulation.data_loader import load_json_data, load_stop_data, extract_route_stops, load_stop_mapping
from simulation.timetable import load_timetable, TimetableManager
from simulation.bus_stop import BusStop
from simulation.bus import Bus
from simulation.scheduling import schedule_bus_dispatch
from simulation.event_handler import EventHandler
from simulation.logger_config import get_logger, setup_logging
from simulation.config import DISPATCH_INTERVAL_SECONDS, BUS_CAPACITY

# Initialize logging as early as possible, right after imports.
setup_logging()

# Use only libsumo
import libsumo as traci

# Global variable to store the active scheduler instance
_active_scheduler = None

def get_active_scheduler():
    """Gets the currently active scheduler instance."""
    return _active_scheduler

def set_active_scheduler(scheduler):
    """Sets the currently active scheduler instance."""
    global _active_scheduler
    _active_scheduler = scheduler

class SimulationRunner:
    """Orchestrates the setup and execution of the bus simulation, integrated with SUMO."""
    
    def __init__(self, config_path: str = 'config.yml', scenario_name: str = '601'):
        self.logger = get_logger(self.__class__.__name__)
        
        # Load configuration from YAML
        try:
            config_file = Path(config_path)
            with open(config_file, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
        except (FileNotFoundError, yaml.YAMLError) as e:
            self.logger.error(f"Failed to load or parse config file {config_path}: {e}")
            sys.exit(1)

        self.scenario_name = scenario_name
        self.scenario_config = self.config['scenarios'][self.scenario_name]

        self.env = simpy.Environment()
        self.bus_stops: Dict[str, BusStop] = {}
        self.active_buses: List[Bus] = []
        
        # Check if data collection needs to be enabled (for adaptive scheduler)
        scheduler_type = self.config.get('simpy', {}).get('scheduler', {}).get('type', 'timetable')
        enable_data_collection = scheduler_type in ['adaptive', 'adaptive_headway']
        
        # Get the enable_kpi config for the corresponding scheduler type
        if scheduler_type == 'adaptive':
            enable_kpi = enable_data_collection and self.config.get('simpy', {}).get('scheduler', {}).get('adaptive', {}).get('enable_kpi', True)
        elif scheduler_type == 'adaptive_headway':
            enable_kpi = enable_data_collection and self.config.get('simpy', {}).get('scheduler', {}).get('adaptive_headway', {}).get('enable_kpi', True)
        else:
            enable_kpi = False
        
        self.event_handler = EventHandler(self.env, enable_data_collection=enable_data_collection, enable_kpi=enable_kpi)
        self.timetable_data: Optional[Dict[str, List[Dict[str, Any]]]] = None
        # Stop mapping now supports bidirectional mapping format:
        # {
        #   'simpy_to_sumo': {
        #     'northbound': {'simpy_id': 'sumo_id', ...},
        #     'southbound': {'simpy_id': 'sumo_id', ...}
        #   },
        #   'sumo_routes': {'route_id': {'northbound': 'sumo_route', 'southbound': 'sumo_route'}}
        # }
        self.stop_mapping: Dict[str, str] = {}
        
        # Randomly select a date for the simulation run
        self.selected_month = random.randint(1, 12)
        self.selected_day = random.randint(1, 28)
        self.logger.info(f"--- Running simulation for scenario '{self.scenario_name}' ---")
        self.logger.info(f"--- Selected random date: Month {self.selected_month}, Day {self.selected_day} ---")
    
    def _start_sumo(self):
        """Starts SUMO using libsumo (no GUI support)."""
        self.logger.info("Starting SUMO with libsumo...")
        sumo_config_path = self.config['paths']['sumo_scenario']['config_file'].format(scenario_name=self.scenario_name)
        
        # Use sumo instead of sumo-gui
        sumo_binary = "sumo"  # Force use of non-GUI version
        
        sumo_cmd = [
            sumo_binary,
            "-c", sumo_config_path,
            "--seed", str(self.config['simulation']['random_seed']),
            "--time-to-teleport", "-1",  # Disable teleporting
            "--time-to-teleport.highways", "-1",  # No teleporting on highways
            "--collision.action", "remove",  # Remove vehicles on collision
            "--collision-output", os.path.join(self.config['paths']['output']['log_dir'], "collisions.xml"),  # Log collision information
            "--collision.check-junctions", "true",  # Check for collisions at junctions
            "--no-step-log",
            "--quit-on-end"
        ]
        
        try:
            self.logger.info(f"Starting SUMO with libsumo, command: {' '.join(sumo_cmd)}")
            traci.start(sumo_cmd)
            self.logger.info("libsumo connection started successfully.")
            
            # Test connection
            version = traci.getVersion()
            self.logger.info(f"Connected to SUMO version: {version}")
            
        except Exception as e:
            self.logger.error(f"Failed to start SUMO with libsumo: {e}", exc_info=True)
            sys.exit(1)

    def load_data(self) -> tuple[Dict, Dict, Dict, Optional[Dict]] | tuple[None, None, None, None]:
        """Loads all required data files for the simulation based on config."""
        self.logger.info("Loading data...")
        paths = self.config['paths']['simpy_data']
        
        def get_path(file_key: str) -> str:
            return paths[file_key].format(scenario_name=self.scenario_name)

        try:
            arrival_data = load_json_data(get_path('arrival_rates_file'))
            weight_data = load_json_data(get_path('destination_weights_file'))
            stops_routes_data = load_stop_data(get_path('stops_file'))

            # Only try to load the default timetable file if it exists, to avoid false alarms
            default_timetable_path = get_path('timetable_file')
            if os.path.exists(default_timetable_path):
                self.timetable_data = load_timetable(default_timetable_path)
            else:
                self.timetable_data = None
            
            if not all([arrival_data, weight_data, stops_routes_data]):
                self.logger.error("Failed to load one or more essential data files. Exiting.")
                return None, None, None, None
                
            return arrival_data, weight_data, stops_routes_data, self.timetable_data
        except FileNotFoundError as e:
            self.logger.error(f"Data file not found: {e}. Check your config.yml paths. Exiting.")
            return None, None, None, None

    def setup(self) -> bool:
        """Sets up the simulation environment, stops, dispatchers, and starts SUMO."""
        self.logger.info("--- Setting up Simulation --- ")
        
        # Start SUMO first to establish connection
        self._start_sumo()
        
        data_result = self.load_data()
        if data_result[0] is None:
            return False
        
        arrival_data, weight_data, stops_routes_data, timetable_data = data_result
        
        # -------------------------------------------------------------
        # Dynamically determine the start time for passenger generation (based on new logic)
        # -------------------------------------------------------------
        configured_start_time = self.config['simulation'].get('start_time_seconds', 0)
        earliest_departure = None
        latest_departure = None

        # Scan all timetable files corresponding to the routes to find the earliest and latest departure times
        timetable_dir = Path(f"{self.scenario_name}/timetable")
        if timetable_dir.exists():
            for file in timetable_dir.glob("*_timetable.json"):
                try:
                    import json
                    with open(file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        schedule = data.get('schedule', [])
                        if not schedule:
                            continue
                        # The first stop of each trip is the departure stop
                        for trip in schedule:
                            if trip.get('stops'):
                                dep_time = trip['stops'][0]['scheduled_time']
                                if earliest_departure is None or dep_time < earliest_departure:
                                    earliest_departure = dep_time
                                if latest_departure is None or dep_time > latest_departure:
                                    latest_departure = dep_time
                except Exception as e:
                    self.logger.debug(f"Failed to parse timetable {file}: {e}")

        # New passenger generation logic
        if earliest_departure is not None:
            if configured_start_time < earliest_departure:
                # Simulation start time is earlier than the first bus, start generating passengers at 19000s
                simulation_start_time = 19000
                self.logger.info(
                    f"Simulation start time {configured_start_time}s is earlier than the first bus at {earliest_departure}s, "
                    f"setting passenger generation start time to 19000s"
                )
            else:
                # Simulation start time is later than or equal to the first bus, start generating passengers immediately
                simulation_start_time = configured_start_time
                self.logger.info(
                    f"Simulation start time {configured_start_time}s is later than or equal to the first bus at {earliest_departure}s, "
                    f"passenger generation starts immediately"
                )
        else:
            # No timetable data, use the configured start time
            simulation_start_time = configured_start_time
            self.logger.info(f"No timetable data found, using configured start time {configured_start_time}s")
        
        # Special handling for adaptive_headway mode
        scheduler_type = self.config.get('simpy', {}).get('scheduler', {}).get('type', 'timetable')
        if scheduler_type == 'adaptive_headway' and configured_start_time == 0:
            # adaptive_headway mode and starting from 0s, use a more reasonable passenger generation time
            simulation_start_time = min(660, simulation_start_time)  # Wait at least until 11 minutes
            self.logger.info(
                f"Adjusting passenger generation start time to {simulation_start_time}s in Adaptive headway mode"
            )
        
        # Load SimPy-to-SUMO stop ID mapping
        self.stop_mapping = load_stop_mapping(self.scenario_name)
        if not self.stop_mapping:
            self.logger.error(f"Stop mapping failed to load for scenario {self.scenario_name}. Check mapping file.")
            return False
        
        try:
            all_stop_ids = list(stops_routes_data['stops'].keys())
            routes = extract_route_stops(stops_routes_data['routes'])
        except (KeyError, TypeError) as e:
            self.logger.error(f"Issue reading stops/routes data: {e}. Check format.")
            return False
        
        if not all_stop_ids:
            self.logger.error("No stops defined in the stops data file. Exiting.")
            return False

        self.logger.info(f"Found {len(all_stop_ids)} stops and {len(routes)} routes.")
        
        # Calculate the first service time for each stop
        first_service_time_per_stop: Dict[str, float] = {}
        if timetable_dir.exists():
            for file in timetable_dir.glob("*_timetable.json"):
                try:
                    import json
                    with open(file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        for trip in data.get('schedule', []):
                            stops = trip.get('stops', [])
                            for idx, st in enumerate(stops):
                                stop_id = str(st['stop_id'])
                                t = st['scheduled_time']
                                # Record the earliest arrival time
                                if stop_id not in first_service_time_per_stop or t < first_service_time_per_stop[stop_id]:
                                    first_service_time_per_stop[stop_id] = t
                except Exception as e:
                    self.logger.debug(f"Error scanning timetable {file}: {e}")

        # Save the effective start time for use in run()
        self._effective_start_time = simulation_start_time

        # Create bus stops
        self.logger.info("Creating bus stops...")

        for stop_id in all_stop_ids:
            # For stops with no service records, still use the global simulation_start_time
            stop_start_time = first_service_time_per_stop.get(stop_id, simulation_start_time)

            self.bus_stops[stop_id] = BusStop(
                env=self.env, stop_id=stop_id, all_stop_ids=all_stop_ids,
                routes_data=routes,
                arrival_data=arrival_data, weight_data=weight_data,
                selected_month=self.selected_month,
                selected_day=self.selected_day,
                simulation_start_time=stop_start_time
            )

        # Start dispatchers
        self.logger.info("Starting bus dispatchers...")
        if not routes:
            self.logger.warning("No routes defined. No dispatchers will be started.")
            return True

        target_route_id = self.scenario_config['target_route_id']
        
        # Get scheduler config to decide if dispatch_interval is needed
        scheduler_config = self.config.get('simpy', {}).get('scheduler', {})
        scheduler_type = scheduler_config.get('type', 'auto')
        
        # Only needed for interval or auto mode
        if scheduler_type in ['interval', 'auto']:
            # Prioritize from config file, otherwise use default from config.py
            if 'interval' in scheduler_config and 'default_interval' in scheduler_config['interval']:
                dispatch_interval = scheduler_config['interval']['default_interval']
                self.logger.info(f"Using dispatch interval from config: {dispatch_interval}s")
            else:
                dispatch_interval = DISPATCH_INTERVAL_SECONDS
                self.logger.info(f"Using default dispatch interval from config.py: {dispatch_interval}s")
        else:
            # Use default as fallback for timetable mode
            dispatch_interval = DISPATCH_INTERVAL_SECONDS
            self.logger.info(f"Scheduler type is '{scheduler_type}', dispatch_interval ({dispatch_interval}s) will be used as fallback only")
        
        # Create timetable manager
        timetable_manager = TimetableManager()
        
        for route_id, stop_list in routes.items():
            # Only schedule the target route. If target_route_id is a prefix (e.g., "601" matches "601001/601002"), include it.
            if target_route_id and not route_id.startswith(target_route_id):
                continue

            # Load the corresponding timetable file by direction (route_id)
            timetable = None
            timetable_file = Path(f"{self.scenario_name}/timetable/{route_id}_timetable.json")

            if timetable_file.exists():
                # Load timetable using TimetableManager
                if timetable_manager.load_route_timetable(route_id, timetable_file.as_posix()):
                    timetable = timetable_manager.get_schedule_for_route(route_id)
                    if timetable:
                        self.logger.info(
                            f"Loaded timetable for route {route_id} from {timetable_file} - "
                            f"trips: {len(timetable)}"
                        )
                else:
                    self.logger.warning(
                        f"Failed to load timetable for route {route_id} from {timetable_file}"
                    )
            else:
                self.logger.warning(
                    f"Timetable file not found for route {route_id}: {timetable_file}. "
                    f"Will fall back to default dispatch interval {dispatch_interval}s."
                )

            self.env.process(schedule_bus_dispatch(
                env=self.env,
                route_id=route_id,
                route_stops=stop_list,
                interval=dispatch_interval,
                bus_stops=self.bus_stops,
                active_buses_list=self.active_buses,
                event_handler=self.event_handler,
                timetable=timetable,
                stop_mapping=self.stop_mapping,
                config=self.config,  # Pass the config
                timetable_manager=timetable_manager  # Pass the timetable manager
            ))
        
        self.logger.info("--- Simulation Setup Complete --- ")
        return True
    
    def analyze_results(self):
        """Prints a summary of the simulation results."""
        self.logger.info("\n--- Simulation Finished --- ")
        
        print("\n--- Basic Simulation Results --- ")
        try:
            total_passengers_generated = next(Passenger.id_iter) - 1
        except (StopIteration, AttributeError):
            total_passengers_generated = 0
        print(f"Total passengers generated: {total_passengers_generated}")
        
        if not self.active_buses:
            print("No buses were dispatched during the simulation.")
            return

        print(f"\nAnalysis based on {len(self.active_buses)} bus instances created:")
        bus_capacity = BUS_CAPACITY
        for bus in self.active_buses:
            print(f"\nBus {bus.bus_id} Log ({'Failed' if bus.failed else 'Completed/Running'}):")
            if not bus.log:
                print("  No events recorded.")
                continue
            
            log_preview = bus.log[:5] + [("...",)] + bus.log[-5:] if len(bus.log) > 10 else bus.log
            for event in log_preview:
                print(f"  {event}")
            
            if not bus.failed or any(log[1] == "end_route" for log in bus.log):
                final_load = bus.current_capacity_load
                final_disabled = bus.disabled_passenger_count
                print(f"  Final Load: {final_load}/{bus_capacity} ({final_disabled} disabled)")
            else:
                print("  Bus failed mid-route.")
        
        print("\n--- Event Summary ---")
        event_summary = self.event_handler.get_event_summary()
        if event_summary:
            total_events = sum(event_summary.values())
            print(f"Total events recorded: {total_events}")
            for event_type, count in sorted(event_summary.items()):
                percentage = (count / total_events * 100) if total_events > 0 else 0
                print(f"  {event_type}: {count} ({percentage:.1f}%)")
        else:
            print("  No events recorded.")

    def run(self):
        """Runs the co-simulation loop for SimPy and SUMO."""
        if not self.setup():
            self.logger.error("Simulation setup failed. Exiting.")
            return

        self.logger.info("\n--- Running Co-Simulation Loop ---")
        start_time = getattr(self, '_effective_start_time', self.config['simulation'].get('start_time_seconds', 0))
        end_time = self.config['simulation']['end_time_seconds']
        
        self.logger.info(f"{'+'*80}")
        self.logger.info(f"SIMULATION STARTED")
        duration = end_time - start_time
        self.logger.info(f"  Running from {start_time:.0f}s to {end_time:.0f}s")
        self.logger.info(f"  Duration: {duration:.0f} seconds ({duration/3600:.1f} hours)")
        self.logger.info(f"  Active Buses: {len(self.active_buses)}")
        self.logger.info(f"  Total Stops: {len(self.bus_stops)}")
        self.logger.info(f"  Random Date: Month {self.selected_month}, Day {self.selected_day}")
        self.logger.info(f"{'+'*80}")
        
        # Define SUMO's step length (in seconds)
        sumo_step_length = 1.0  # 1-second step length, can be adjusted as needed
        
        try:
            # Initialize simulation time to start_time
            simulation_time = start_time
            last_simpy_time = start_time
            
            # Set both SUMO and SimPy time to start_time
            if start_time > 0:
                # Fast-forward SUMO to start_time
                self.logger.info(f"Fast-forwarding simulation to start time: {start_time}s")
                while traci.simulation.getTime() < start_time:
                    traci.simulationStep()
                
                # Synchronize SimPy time
                self.env.run(until=start_time)
            
            # Main loop runs from start_time to end_time
            while simulation_time < end_time:
                # Check if the connection is still valid
                try:
                    # First, try a simple operation to test the connection
                    _ = traci.simulation.getTime()
                except Exception as e:
                    self.logger.error(f"Lost connection to SUMO: {e}")
                    break
                
                # Advance SUMO by one step
                traci.simulationStep()
                current_sumo_time = traci.simulation.getTime()
                
                # Synchronize SimPy time to the current SUMO time
                # Run all due SimPy events (including vehicle monitoring processes)
                if self.env.now < current_sumo_time:
                    try:
                        self.env.run(until=current_sumo_time)
                    except simpy.core.EmptySchedule:
                        # No more events scheduled, continue
                        pass
                
                # Dynamically update bus_map (as new buses may be added by the monitoring process)
                bus_map = {bus.bus_id: bus for bus in self.active_buses if bus.state != "Finished"}
                
                # Check the status of all buses
                for bus_id in list(bus_map.keys()):  # Use list() to avoid modifying the dictionary while iterating
                    if bus_id not in traci.vehicle.getIDList():
                        # Vehicle is no longer in SUMO
                        continue
                    
                    simpy_bus = bus_map[bus_id]
                    
                    # Get real-time status information for the bus
                    try:
                        # Get bus position, speed, etc.
                        position = traci.vehicle.getPosition(bus_id)
                        speed = traci.vehicle.getSpeed(bus_id)
                        road_id = traci.vehicle.getRoadID(bus_id)
                        route_progress = traci.vehicle.getRouteIndex(bus_id)
                        
                                                # Get bus stop state (more strict detection)
                        is_at_bus_stop = False

                        if hasattr(traci.vehicle, 'getStopState'):
                            try:
                                stop_state = traci.vehicle.getStopState(bus_id)
                                # Only check bit-0: vehicle has arrived and is waiting at the stop.
                                is_at_bus_stop = (stop_state & 1) != 0
                            except traci.TraCIException:
                                pass

                        # Arrival determination now only depends on the physical arrival state.
                        is_at_correct_stop = is_at_bus_stop
                        
                        # Output detailed status periodically (to avoid excessive output)
                        if int(current_sumo_time) % 30 == 0 and int(current_sumo_time) != int(last_simpy_time):  # Output once every 30 seconds
                            self.logger.info(f"Bus {bus_id} Status - Time: {current_sumo_time:.1f}s, "
                                           f"Position: ({position[0]:.1f}, {position[1]:.1f}), "
                                           f"Speed: {speed:.1f} m/s, Road: {road_id}, "
                                           f"State: {simpy_bus.state}, "
                                           f"Load: {simpy_bus.current_capacity_load}/{simpy_bus.bus_capacity}, "
                                           f"Stop Index: {simpy_bus.current_stop_index}/{len(simpy_bus.route_stops)-1}")
                        
                        # Detect stuck vehicles
                        if speed == 0 and simpy_bus.state == "EnRoute":
                            if not hasattr(simpy_bus, '_stuck_start_time'):
                                simpy_bus._stuck_start_time = current_sumo_time
                            elif current_sumo_time - simpy_bus._stuck_start_time > 30:  # Stuck for more than 30 seconds
                                # Get more diagnostic information
                                try:
                                    # Get information about the vehicle ahead
                                    leader_info = traci.vehicle.getLeader(bus_id, 100) if hasattr(traci.vehicle, 'getLeader') else None
                                    waiting_time = traci.vehicle.getWaitingTime(bus_id) if hasattr(traci.vehicle, 'getWaitingTime') else 0
                                    accumulated_waiting = traci.vehicle.getAccumulatedWaitingTime(bus_id) if hasattr(traci.vehicle, 'getAccumulatedWaitingTime') else 0
                                    
                                    self.logger.warning(f"Bus {bus_id} STUCK for {current_sumo_time - simpy_bus._stuck_start_time:.1f}s at {road_id}")
                                    self.logger.warning(f"  Waiting time: {waiting_time:.1f}s, Accumulated: {accumulated_waiting:.1f}s")
                                    if leader_info:
                                        self.logger.warning(f"  Leader vehicle: {leader_info[0]} at distance {leader_info[1]:.1f}m")
                                    
                                    # Reset the timer to avoid repeated output
                                    simpy_bus._stuck_start_time = current_sumo_time
                                except Exception as e:
                                    self.logger.debug(f"Could not get diagnostic info for stuck bus: {e}")
                        elif speed > 0 and hasattr(simpy_bus, '_stuck_start_time'):
                            # The vehicle has started moving, clear the stuck flag
                            delattr(simpy_bus, '_stuck_start_time')
                        
                        # Check if the route is complete
                        if simpy_bus.state == "Finished":
                            self.logger.info(f"{'$'*60}")
                            self.logger.info(f"Bus {bus_id} COMPLETED ROUTE")
                            self.logger.info(f"  Time: {current_sumo_time:.1f}s")
                            self.logger.info(f"  Final Load: {simpy_bus.current_capacity_load}/{simpy_bus.bus_capacity}")
                            self.logger.info(f"  Total Stops Served: {len(simpy_bus.route_stops)}")
                            self.logger.info(f"{'$'*60}")
                            # Remove the completed bus from bus_map to avoid repeated output
                            del bus_map[bus_id]
                            continue
                        
                        if is_at_correct_stop and simpy_bus.state == "EnRoute":
                            # The bus just arrived at the stop
                            expected_stop_id = simpy_bus.current_stop_id()
                            
                            # If the stop is already being processed, skip (to avoid duplicate processing)
                            if simpy_bus.processing_stop:
                                continue
                            
                            # Check if this is a new arrival event
                            # Use the current stop index instead of the ID to detect duplicates
                            current_stop_index = simpy_bus.current_stop_index
                            if not hasattr(simpy_bus, '_last_processed_stop_index') or \
                               simpy_bus._last_processed_stop_index != current_stop_index:
                                simpy_bus._last_processed_stop_index = current_stop_index
                                simpy_bus._last_arrived_stop_id = expected_stop_id  # Keep for departure detection
                                
                                # Output detailed status on arrival
                                self.logger.info(f"{'='*60}")
                                self.logger.info(f"Bus {bus_id} ARRIVED at stop {expected_stop_id}")
                                self.logger.info(f"  Time: {current_sumo_time:.1f}s")
                                self.logger.info(f"  Position: ({position[0]:.1f}, {position[1]:.1f})")
                                self.logger.info(f"  Current Load: {simpy_bus.current_capacity_load}/{simpy_bus.bus_capacity}")
                                self.logger.info(f"  Disabled Passengers: {simpy_bus.disabled_passenger_count}")
                                self.logger.info(f"  Stop Progress: {simpy_bus.current_stop_index+1}/{len(simpy_bus.route_stops)}")
                                self.logger.info(f"{'='*60}")
                                
                                simpy_bus.handle_sumo_stop_arrival(expected_stop_id)
                                
                                # Give SimPy some time to process the arrival event
                                while self.env.peek() == self.env.now:
                                    self.env.step()
                        
                        elif not is_at_correct_stop and hasattr(simpy_bus, '_last_arrived_stop_id'):
                            # The bus has left the stop
                            # Check if stop processing is complete
                            if hasattr(simpy_bus, 'stop_processing_complete') and not simpy_bus.stop_processing_complete:
                                # Stop processing is not yet complete, continue waiting
                                continue
                            
                            # Get the ID of the stop that was just departed
                            departed_stop_id = simpy_bus._last_arrived_stop_id
                            
                            # Clear the flags in preparation for the next arrival
                            delattr(simpy_bus, '_last_arrived_stop_id')
                            if hasattr(simpy_bus, '_last_processed_stop_index'):
                                delattr(simpy_bus, '_last_processed_stop_index')
                            
                            # If the bus was waiting to leave the stop, the index can now be updated
                            if hasattr(simpy_bus, '_waiting_to_leave_stop') and simpy_bus._waiting_to_leave_stop:
                                if hasattr(simpy_bus, '_completed_stop_id') and simpy_bus._completed_stop_id == departed_stop_id:
                                    # It is now safe to update the index
                                    if simpy_bus.current_stop_index < len(simpy_bus.route_stops) - 1:
                                        simpy_bus.current_stop_index += 1
                                        self.logger.info(f"Bus {bus_id} index updated after departure: now at index {simpy_bus.current_stop_index}")
                                    
                                    # Clear the waiting flag
                                    simpy_bus._waiting_to_leave_stop = False
                                    delattr(simpy_bus, '_completed_stop_id')
                            
                            self.logger.info(f"{'*'*60}")
                            self.logger.info(f"Bus {bus_id} DEPARTED from stop {departed_stop_id}")
                            self.logger.info(f"  Time: {current_sumo_time:.1f}s")
                            self.logger.info(f"  Current Load: {simpy_bus.current_capacity_load}/{simpy_bus.bus_capacity}")
                            self.logger.info(f"  Current stop index: {simpy_bus.current_stop_index}")
                            self.logger.info(f"  Next Stop: {simpy_bus.current_stop_id() if simpy_bus.current_stop_index < len(simpy_bus.route_stops) else 'Route Complete'}")
                            self.logger.info(f"{'*'*60}")
                            
                            # Record the departure event
                            if simpy_bus.event_handler:
                                simpy_bus.event_handler.record_event(current_sumo_time, 'bus_departed', {
                                    'bus_id': bus_id,
                                    'stop_id': departed_stop_id,
                                    'load': simpy_bus.current_capacity_load,
                                    'disabled_count': simpy_bus.disabled_passenger_count
                                })
                    
                    except traci.TraCIException as e:
                        self.logger.warning(f"Error getting stop state for bus {bus_id}: {e}")
                        continue
                
                # Process any due SimPy events (such as passenger generation)
                while self.env.peek() < current_sumo_time:
                    try:
                        self.env.step()
                    except simpy.core.EmptySchedule:
                        break
                
                # Update simulation time
                simulation_time = current_sumo_time
                last_simpy_time = current_sumo_time
                
                # Output progress periodically
                if int(simulation_time) % 300 == 0 and simulation_time > start_time:  # Every 5 minutes
                    elapsed = simulation_time - start_time
                    progress_percent = (elapsed / duration * 100) if duration > 0 else 0
                    self.logger.info(f"Simulation progress: {elapsed:.0f}/{duration:.0f} seconds ({progress_percent:.1f}%) - Time: {simulation_time:.0f}s")
                    self.logger.info(f"  Active buses in simulation: {len([b for b in self.active_buses if b.state != 'Finished'])}")

        except Exception as e:
            self.logger.error(f"An error occurred during the co-simulation loop: {e}", exc_info=True)
        finally:
            self.logger.info("--- Co-Simulation Loop Finished ---")
            try:
                traci.close()
                self.logger.info("SUMO connection closed.")
            except Exception as e:
                self.logger.warning(f"Error closing SUMO connection: {e}")
        
        self.analyze_results()

    def process_sumo_events(self, results: Dict[str, Any], bus_map: Dict[str, Bus]):
        """Processes subscription results from SUMO and triggers SimPy events."""
        for bus_id, data in results.items():
            simpy_bus = bus_map.get(bus_id)
            if not simpy_bus or simpy_bus.state != "EnRoute":
                continue

            stop_state = data.get(traci.constants.VAR_STOP_STATE)
            
            is_at_bus_stop = (stop_state & 1) != 0
            
            if is_at_bus_stop:
                current_stop_id = simpy_bus.current_stop_id()
                if not hasattr(simpy_bus, '_last_triggered_stop') or simpy_bus._last_triggered_stop != current_stop_id:
                    self.logger.info(f"SUMO event: Bus {bus_id} arrived at stop {current_stop_id}.")
                    simpy_bus._last_triggered_stop = current_stop_id
                    simpy_bus.handle_sumo_stop_arrival(current_stop_id)

def run_scheduler_tests(args):
    """Runs scheduler tests."""
    logger = get_logger(__name__)
    
    if args.test_adaptive:
        # Test only the adaptive scheduler
        logger.info("=" * 80)
        logger.info("Adaptive Scheduler Functionality Test")
        logger.info("=" * 80)
        
        # Modify the configuration to adaptive scheduling
        with open(args.config, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # Set the adaptive scheduler
        config['simpy']['scheduler']['type'] = 'adaptive'
        config['simpy']['scheduler']['adaptive']['demand_threshold'] = 20  # Lower the threshold
        config['simulation']['end_time_seconds'] = 10800  # 3 hours
        config['simulation']['num_rounds'] = 1
        
        # Write the configuration back
        with open(args.config, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
        
        logger.info(f"Configuration Parameters:")
        logger.info(f"  Scheduler Type: Adaptive (adaptive)")
        logger.info(f"  Demand Threshold: {config['simpy']['scheduler']['adaptive']['demand_threshold']} people")
        logger.info(f"  Simulation Duration: {config['simulation']['end_time_seconds']} seconds")
        
        # Run the simulation
        runner = SimulationRunner(config_path=args.config, scenario_name=args.scenario)
        logger.info("Starting adaptive scheduling simulation...")
        start_time = time.time()
        
        try:
            runner.run()
            end_time = time.time()
            duration = end_time - start_time
            
            logger.info(f"\nSimulation complete!")
            logger.info(f"Total time elapsed: {duration:.2f} seconds")
            
            # If KPIs are enabled, display the final report
            if hasattr(runner.event_handler, 'current_kpis') and runner.event_handler.current_kpis:
                kpis = runner.event_handler.current_kpis
                logger.info("\nFinal KPI Report:")
                logger.info(f"  On-Time Performance (OTP): {kpis.get('otp', 0):.2%}")
                logger.info(f"  Average Wait Time: {kpis.get('avg_wait_time', 0):.1f} seconds")
                logger.info(f"  Headway Regularity: {kpis.get('headway_regularity', 0):.2f}")
                logger.info(f"  Passenger Satisfaction: {kpis.get('passenger_satisfaction', 0):.2f}")
                logger.info(f"  System Efficiency: {kpis.get('system_efficiency', 0):.2f}")
            
        except Exception as e:
            logger.error(f"Simulation run failed: {e}", exc_info=True)
    
    elif args.compare_schedulers:
        # Comparison test
        logger.info("=" * 80)
        logger.info("Scheduler Comparison Test")
        logger.info("=" * 80)
        
        scenarios = {
            "timetable": "Fixed Timetable Scheduling",
            "adaptive": "Adaptive Dynamic Scheduling"
        }
        
        results = {}
        
        for scheduler_type, name in scenarios.items():
            logger.info(f"\nRunning test: {name}")
            logger.info("-" * 60)
            
            # Modify the configuration
            with open(args.config, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            config['simpy']['scheduler']['type'] = scheduler_type
            config['simulation']['end_time_seconds'] = 7200  # 2 hours
            config['simulation']['num_rounds'] = 1
            
            with open(args.config, 'w', encoding='utf-8') as f:
                yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
            
            # Run the simulation
            start_time = time.time()
            runner = SimulationRunner(config_path=args.config, scenario_name=args.scenario)
            
            try:
                runner.run()
                end_time = time.time()
                duration = end_time - start_time
                
                # Collect the results
                event_summary = runner.event_handler.get_event_summary()
                
                result = {
                    'duration': duration,
                    'events': event_summary,
                    'name': name
                }
                
                # Also collect KPI data if available
                if hasattr(runner.event_handler, 'current_kpis'):
                    result['kpis'] = runner.event_handler.current_kpis
                
                results[scheduler_type] = result
                logger.info(f"Test complete, time elapsed: {duration:.2f} seconds")
                
            except Exception as e:
                logger.error(f"Test failed: {e}")
                results[scheduler_type] = {'error': str(e), 'name': name}
        
        # Output the comparison results
        logger.info("\n" + "=" * 80)
        logger.info("Test Results Comparison")
        logger.info("=" * 80)
        
        for scheduler_type, result in results.items():
            logger.info(f"\n{result['name']}:")
            
            if 'error' in result:
                logger.info(f"  Error: {result['error']}")
                continue
                
            logger.info(f"  Run Time: {result['duration']:.2f} seconds")
            logger.info(f"  Total Events: {sum(result['events'].values())}")
            
            if 'kpis' in result and result['kpis']:
                kpis = result['kpis']
                logger.info(f"  On-Time Performance (OTP): {kpis.get('otp', 0):.2%}")
                logger.info(f"  Average Wait Time: {kpis.get('avg_wait_time', 0):.1f} seconds")
                logger.info(f"  Headway Regularity: {kpis.get('headway_regularity', 0):.2f}")
                logger.info(f"  Passenger Satisfaction: {kpis.get('passenger_satisfaction', 0):.2f}")
                logger.info(f"  System Efficiency: {kpis.get('system_efficiency', 0):.2f}")


def main():
    """Main entry point for the simulation with real-time ETL."""

    import argparse

    parser = argparse.ArgumentParser(description="Simulation runner with streaming ETL")
    parser.add_argument("--scenario", default="601", help="Scenario name (e.g. 601)")
    parser.add_argument("--config", default="config.yml", help="Config YAML path")
    parser.add_argument("--etl-config", default="config/db.yml", help="ETL DB config path")
    parser.add_argument("--data-dir", default="./simulation_data", help="Directory to write event files")
    parser.add_argument("--batch-size", type=int, default=500, help="Event file batch size for SimPyDataHook")
    parser.add_argument("--test-adaptive", action="store_true", help="Test adaptive scheduler")
    parser.add_argument("--compare-schedulers", action="store_true", help="Compare different scheduler types")
    args = parser.parse_args()

    # Handle test modes
    if args.test_adaptive or args.compare_schedulers:
        run_scheduler_tests(args)
        return

    # Read the config file to get the number of simulation rounds and data target
    try:
        with open(args.config, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        num_rounds = config.get('simulation', {}).get('num_rounds', 1)
        base_random_seed = config.get('simulation', {}).get('random_seed', 42)
        data_target = config.get('simulation', {}).get('data_target', 'scenario')
    except Exception as e:
        logger.error(f"Failed to load config file: {e}")
        num_rounds = 1
        base_random_seed = 42
        data_target = 'scenario'

    logger = get_logger(__name__)
    logger.info(f"{'='*80}")
    logger.info(f"Starting multi-round simulation, total rounds: {num_rounds}")
    logger.info(f"Data will be written to the: {'BusTrip' if data_target == 'scenario' else 'Baseline'} table")
    logger.info(f"{'='*80}")

    # Execute multi-round simulation
    for round_num in range(1, num_rounds + 1):
        logger.info(f"\n{'#'*80}")
        logger.info(f"Starting simulation round {round_num}/{num_rounds}")
        logger.info(f"{'#'*80}")

        # Generate a new random seed for each round
        current_seed = base_random_seed + round_num - 1
        
        # Update the random seed in the config file
        config['simulation']['random_seed'] = current_seed
        with open(args.config, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, default_flow_style=False)

        # --- 1. Initialize SimulationRunner ---
        simulation = SimulationRunner(config_path=args.config, scenario_name=args.scenario)
        simulation.logger.info(f"Round {round_num} simulation - Random seed: {current_seed}, "
                             f"Date: Month {simulation.selected_month}, Day {simulation.selected_day}")

        # --- 2. Integrate data collection hook ---
        from sim_hook.enhanced_integration import integrate_data_collection
        data_hook = integrate_data_collection(simulation, output_dir=args.data_dir)

        # --- 3. Asynchronously start streaming ETL (Structured Streaming) ---
        import threading
        from spark_jobs.etl_trip_metrics import BusTripETL
        
        # Only reset the database on the first round
        reset_database = (round_num == 1)
        
        etl = BusTripETL(
            config_path=args.etl_config, 
            reset_database=reset_database,
            data_target=data_target
        )
        
        if reset_database:
            simulation.logger.info(f"First round simulation: {'BusTrip' if data_target == 'scenario' else 'Baseline'} table has been reset")
        else:
            simulation.logger.info(f"Round {round_num} simulation: Retaining existing database data")
        
        # Start the streaming query in a background thread to avoid blocking the main thread
        streaming_query = None
        etl_thread = None
        
        def start_streaming_etl():
            nonlocal streaming_query
            try:
                simulation.logger.info("Starting streaming ETL in a background thread...")
                streaming_query = etl.run_streaming(args.data_dir)
                simulation.logger.info("Streaming ETL has started in the background")
            except Exception as e:
                simulation.logger.error(f"Failed to start streaming ETL: {e}", exc_info=True)
        
        # Start the ETL thread
        etl_thread = threading.Thread(target=start_streaming_etl, daemon=True)
        etl_thread.start()
        
        # Wait for the ETL thread to start (up to 10 seconds)
        etl_thread.join(timeout=10)
        
        if etl_thread.is_alive():
            simulation.logger.info("ETL is initializing in the background, continuing to start the simulation...")
        else:
            simulation.logger.info("ETL thread has completed initialization")

        try:
            # --- 4. Run the simulation ---
            simulation.run()
            
            simulation.logger.info(f"Round {round_num} simulation complete")

        except Exception as e:
            simulation.logger.error(f"Round {round_num} simulation encountered an error: {e}", exc_info=True)

        finally:
            # --- 5. Graceful shutdown ---
            simulation.logger.info(f"Cleaning up resources for round {round_num}...")

            # Stop the data collection hook
            try:
                data_hook.stop()
            except Exception as e:
                simulation.logger.warning(f"Failed to stop data hook gracefully: {e}")

            # Process remaining files and stop the streaming query
            try:
                # Wait for the ETL thread to complete initialization (if it's still running)
                if etl_thread and etl_thread.is_alive():
                    simulation.logger.info("Waiting for ETL thread to complete initialization...")
                    etl_thread.join(timeout=30)  # Wait for up to 30 seconds
                
                # If streaming_query was initialized, stop it
                if streaming_query:
                    if hasattr(streaming_query, "processAllAvailable"):
                        simulation.logger.info("Processing remaining streaming data...")
                        streaming_query.processAllAvailable()
                    simulation.logger.info("Stopping streaming query...")
                    streaming_query.stop()
                    # Wait for query to fully terminate to prevent BlockManager exceptions from Spark heartbeat
                    try:
                        streaming_query.awaitTermination(60000)  # Maximum wait 60 seconds
                        simulation.logger.info("Streaming query terminated cleanly")
                    except Exception as e:
                        simulation.logger.warning(f"Streaming query termination timed out or failed: {e}")
                else:
                    simulation.logger.warning("Streaming query was not initialized or has already stopped")

                # --- NEW: Additionally run a batch process to ensure all event files are written to the database ---
                try:
                    simulation.logger.info("Executing batch ETL to supplement unprocessed data...")
                    etl.run_batch(os.path.join(args.data_dir, "*.json"), mode="append")
                    simulation.logger.info("Batch supplement complete")
                except Exception as e:
                    simulation.logger.warning(f"Batch supplement failed: {e}")
            except Exception as e:
                simulation.logger.warning(f"Failed to stop streaming query: {e}")

            # Close the Spark session
            try:
                etl.close()
            except Exception as e:
                simulation.logger.warning(f"Failed to close Spark session: {e}")

            # --- 6. Clean up temporary directories ---
            try:
                # The simulation_data directory is provided as a parameter
                dirs_to_clean = [
                    os.path.abspath(args.data_dir),
                    # The checkpoint directory is read from the ETL config, or defaults to ./checkpoint if not configured
                    os.path.abspath(etl.config.get('etl', {}).get('checkpoint_dir', './checkpoint'))
                ]
                for dir_path in dirs_to_clean:
                    if os.path.exists(dir_path):
                        shutil.rmtree(dir_path, ignore_errors=True)
                        os.makedirs(dir_path, exist_ok=True)  # Keep the empty directory for the next run
                        simulation.logger.info(f"Directory cleaned: {dir_path}")
            except Exception as e:
                simulation.logger.warning(f"Failed to clean directories: {e}")

            simulation.logger.info(f"Cleanup for round {round_num} complete")
            
            # If it's not the last round, wait a bit before starting the next
            if round_num < num_rounds:
                simulation.logger.info(f"Waiting 5 seconds before starting the next simulation round...")
                time.sleep(5)

    # Restore the original random seed in the config file
    config['simulation']['random_seed'] = base_random_seed
    with open(args.config, 'w', encoding='utf-8') as f:
        yaml.dump(config, f, default_flow_style=False)

    logger.info(f"\n{'='*80}")
    logger.info(f"All {num_rounds} rounds of simulation have been completed")
    logger.info(f"{'='*80}")

if __name__ == "__main__":
    main() 