"""
Enhanced integration module
Correctly integrates data collection hooks into existing bus simulation systems
"""
import sys
sys.path.append('..')

from typing import Dict, List, Any, Optional, TYPE_CHECKING
import simpy
from pathlib import Path
import logging

from simulation.bus import Bus
from simulation.bus_stop import BusStop
from simulation.scheduling import create_bus_in_sumo, get_route_from_stops
from sim_hook.hook import SimPyDataHook

if TYPE_CHECKING:
    from simulation.event_handler import EventHandler

# According to memory hint, use libsumo instead of traci
import libsumo as traci

logger = logging.getLogger(__name__)


class DataCollectingBus(Bus):
    """Bus class with integrated data collection functionality."""
    
    def __init__(self, *args, data_hook: SimPyDataHook = None, **kwargs):
        # Call the parent class constructor first to let it handle trip_data
        super().__init__(*args, **kwargs)
        
        # Only add data collection related attributes
        self.data_hook = data_hook
        self._arrival_time = None
        
        # Use the parent's stop_schedules, no need to parse trip_data again
        # The parent Bus.__init__ has already parsed trip_data['stops'] into self.stop_schedules
        
        # Debug: Print timetable information
        if self.stop_schedules:
            logger.debug(f"Bus {self.bus_id} initialized with {len(self.stop_schedules)} scheduled stops")
            # Print the scheduled times of the first 3 stops as an example
            sample_stops = list(self.stop_schedules.items())[:3]
            for stop_id, sched_time in sample_stops:
                logger.debug(f"  Stop {stop_id}: scheduled at {sched_time}s")
        else:
            logger.warning(f"Bus {self.bus_id} has no scheduled stops in stop_schedules")
        
    def handle_sumo_stop_arrival(self, stop_id: str):
        """Overrides the stop arrival handling method to add data collection."""
        self._arrival_time = self.env.now
        
        # Data collection - arrival event
        if self.data_hook and self.state == "EnRoute":
            try:
                # Get direction
                direction = "inbound" if "_001" in self.route_id or "_1" in self.route_id else "outbound"
                
                # Use the parent's stop_schedules to get the scheduled time
                scheduled_time = self.stop_schedules.get(stop_id)
                if scheduled_time is None:
                    logger.warning(
                        f"Scheduled arrival time missing for stop {stop_id} in trip {self.bus_id}_{self.route_id}."
                    )
                
                # Record arrival event
                self.data_hook.on_bus_arrival(
                    env_time=self.env.now,
                    bus_id=self.bus_id,
                    route_id=self.route_id.split('_')[0],  # Extract base route ID like "601"
                    stop_id=stop_id,
                    stop_sequence=self.current_stop_index + 1,
                    scheduled_time=scheduled_time,
                    passenger_load=self.current_capacity_load,
                    wheelchair_count=self.disabled_passenger_count,
                    is_replacement="_Repl_" in self.bus_id,
                    direction=direction,
                    speed=0,  # Speed is 0 on arrival
                    distance_to_next=self._calculate_distance_to_next(),
                    distance_to_trip=self._calculate_distance_to_trip()
                )
            except Exception as e:
                logger.error(f"Data collection failed (arrival): {e}", exc_info=True)
        
        # Call parent method
        super().handle_sumo_stop_arrival(stop_id)
        
    def _process_stop_arrival(self, bus_stop: BusStop, stop_id: str):
        """Overrides stop processing method to add departure data collection."""
        # Record the load before processing
        initial_load = self.current_capacity_load
        
        # Call parent method
        result = yield from super()._process_stop_arrival(bus_stop, stop_id)
        
        # Data collection - departure event
        if self.data_hook:
            try:
                # Calculate dwell time
                dwell_time = self.env.now - self._arrival_time if self._arrival_time else 0
                
                # Calculate number of passengers boarded and alighted
                boarded = 0
                alighted = 0
                if result is not None:
                    # Parent bus._process_stop_arrival returns (alighted, boarded)
                    try:
                        alighted = result[0]
                        boarded = result[1] if len(result) > 1 else 0
                    except Exception:
                        # Keep default value of 0 if result format is unexpected
                        pass
                
                # Record departure event
                self.data_hook.on_bus_departure(
                    env_time=self.env.now,
                    bus_id=self.bus_id,
                    route_id=self.route_id.split('_')[0],
                    stop_id=stop_id,
                    boarded=boarded,
                    alighted=alighted,
                    dwell_time=dwell_time,
                    passenger_load=self.current_capacity_load
                )
            except Exception as e:
                logger.error(f"Data collection failed (departure): {e}", exc_info=True)
                
        return result
        
    def _calculate_distance_to_next(self) -> float:
        """Calculates distance to the next stop (simplified version)."""
        # Based on average distance between stops
        avg_distance = 1000.0  # meters
        return avg_distance
        
    def _calculate_distance_to_trip(self) -> float:
        """Calculates the distance to the final stop."""
        remaining_stops = len(self.route_stops) - self.current_stop_index - 1
        return remaining_stops * 1000.0  # Assuming average stop distance of 1000m


def create_bus_in_sumo_with_data_collection(
    env: simpy.Environment,
    bus_id: str,
    route_id: str,
    route_stops: List[str],
    bus_stops: Dict[str, BusStop],
    active_buses_list: List[Bus],
    event_handler: 'EventHandler',
    trip_data: Dict[str, Any],
    stop_mapping: Dict[str, str],
    scenario_name: str = '601',
    data_hook: Optional[SimPyDataHook] = None
):
    """Creates a Bus with data collection functionality."""
    try:
        # Get SUMO route ID
        sumo_route_id = get_route_from_stops(route_stops, scenario_name, route_id)
        
        # Add vehicle in SUMO
        traci.vehicle.add(
            vehID=bus_id,
            routeID=sumo_route_id,
            typeID="bus",
            depart="now"
        )
        
        logger.info(f"Bus {bus_id} created in SUMO with route {sumo_route_id}")
        
        # Set bus capacity
        try:
            from simulation.config import BUS_CAPACITY
            if hasattr(traci.vehicle, 'setPersonCapacity'):
                traci.vehicle.setPersonCapacity(bus_id, BUS_CAPACITY)
        except Exception:
            pass
        
        # Create Bus object with data collection
        bus = DataCollectingBus(
            env, bus_id, route_id, route_stops, bus_stops, event_handler,
            env.now,  # start_time
            stop_mapping,  # stop_mapping
            trip_data,  # trip_data
            data_hook=data_hook   # data_hook is a keyword argument for the subclass
        )
        
        active_buses_list.append(bus)
        
        # Subscribe to SUMO events
        sub_vars = [
            traci.constants.VAR_ROAD_ID,
            traci.constants.VAR_POSITION,
            traci.constants.VAR_SPEED,
        ]
        if hasattr(traci.constants, 'VAR_STOP_STATE'):
            sub_vars.append(traci.constants.VAR_STOP_STATE)
        traci.vehicle.subscribe(bus_id, sub_vars)
        
        # Start Bus process
        env.process(bus.run())
        
        # Record dispatch event
        event_handler.record_event("bus_dispatched_from_timetable", {
            "bus_id": bus_id,
            "trip_id": trip_data.get('trip_id'),
            "route_id": route_id,
            "sumo_route": sumo_route_id,
            "scheduled_start": trip_data['departure_time']
        })
        
    except traci.TraCIException as e:
        logger.error(f"Failed to create bus {bus_id} in SUMO: {e}")

    return bus  # Return the Bus object for the scheduler to use


def monkey_patch_bus_creation(simulation_runner, data_hook: SimPyDataHook):
    """
    Replaces the bus creation function using monkey patching.
    """
    import simulation.scheduling as scheduling_module
    
    # Save the original function
    original_create_bus = scheduling_module.create_bus_in_sumo
    
    # Create a wrapper function
    def wrapped_create_bus(*args, **kwargs):
        # Add the data_hook parameter and maintain the return value
        return create_bus_in_sumo_with_data_collection(*args, **kwargs, data_hook=data_hook)
    
    # Replace the function
    scheduling_module.create_bus_in_sumo = wrapped_create_bus
    
    logger.info("Bus creation function has been patched to include data collection")
    
    return original_create_bus  # Return the original function for restoration


def integrate_data_collection(simulation_runner, output_dir: str = "./simulation_data"):
    """
    Fully integrates the data collection functionality into the simulation system.
    
    Args:
        simulation_runner: The SimulationRunner instance.
        output_dir: The output directory.
        
    Returns:
        SimPyDataHook: The data collection hook instance.
    """
    # Create the output directory
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    # Get the date selected by the simulator from simulation_runner
    # The year is fixed at 2024 (or can be configured)
    simulation_year = 2024
    simulation_date = f"{simulation_year}-{simulation_runner.selected_month:02d}-{simulation_runner.selected_day:02d}"
    
    # Create the data hook
    data_hook = SimPyDataHook(
        output_dir=output_dir,
        batch_size=100,  # Lower the batch size to write more frequently
        log_level="INFO",
        simulation_date=simulation_date
    )
    
    # Start data collection
    data_hook.start()
    
    # Monkey patch the bus creation function
    original_func = monkey_patch_bus_creation(simulation_runner, data_hook)
    
    # Save the original function reference for later restoration
    simulation_runner._original_create_bus = original_func
    
    logger.info(f"Data collection integrated. Output directory: {output_dir}")
    
    return data_hook 