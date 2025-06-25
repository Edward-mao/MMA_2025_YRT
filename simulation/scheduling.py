# simulation/scheduling.py
import simpy
from typing import List, Dict, Optional, TYPE_CHECKING, Any
import itertools
import os
import sys
import json

# Ensure script can find simulation package
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from .data_loader import load_stop_mapping as load_stop_mapping_from_data
# Import necessary base classes/configs using relative paths
from .bus_stop import BusStop
try:
    from .config import DISPATCH_INTERVAL_SECONDS, TIMETABLE_FILE, BUS_CAPACITY
except ImportError:
    from config import DISPATCH_INTERVAL_SECONDS, TIMETABLE_FILE, BUS_CAPACITY

try:
    from .simulation_utils import format_time # For logging
except ImportError:
    from simulation_utils import format_time

# Import Bus class directly now, as scheduling depends on creating Buses
from .bus import Bus
from .logger_config import get_logger

# Import the new scheduler interface and timetable manager
from .scheduler_interface import SchedulerInterface, TimetableScheduler, IntervalScheduler, AdaptiveScheduler
from .timetable import TimetableManager
from .adaptive_headway_scheduler import AdaptiveHeadwayScheduler

if TYPE_CHECKING:
    from .event_handler import EventHandler

# Use libsumo for performance - required for simulation
import libsumo as traci

# Get logger
logger = get_logger("scheduling")

# Counter for unique bus IDs per route
bus_counters = {} 

def load_stop_mapping(scenario_name: str) -> Dict[str, str]:
    """
    Wrapper function to maintain backward compatibility.
    This function is now deprecated in favor of load_stop_mapping from data_loader.
    """
    logger.warning("The function `load_stop_mapping` in scheduling.py is deprecated. "
                   "Please use `load_stop_mapping` from `data_loader.py` instead.")
    return load_stop_mapping_from_data(scenario_name)

def load_timetable(timetable_file: str) -> Dict[str, Any]:
    """
    Load the timetable from JSON file.
    Deprecated, please use TimetableManager.
    """
    logger.warning("The function `load_timetable` in scheduling.py is deprecated. "
                   "Please use `TimetableManager` from `timetable.py` instead.")
    from .timetable import load_timetable as load_timetable_from_timetable
    return load_timetable_from_timetable(timetable_file) or {}

def get_route_from_stops(route_stops: List[str], scenario_name: str = '601', route_id: str = None) -> str:
    """Get SUMO route ID based on the stops list and route ID."""
    # Load stop mapping to convert SimPy stop IDs to SUMO stop IDs
    stop_mapping = load_stop_mapping_from_data(scenario_name)
    
    # Determine SUMO route based on route_id
    if route_id:
        # Check if it's northbound or southbound based on route_id
        if route_id.endswith('001') or 'northbound' in route_id.lower():
            return "1875876"  # Northbound route
        elif route_id.endswith('002') or 'southbound' in route_id.lower():
            return "1875927"  # Southbound route
    
    # Fallback: determine based on first stop
    if route_stops:
        first_stop = route_stops[0]
        # If first stop is FINCH (9769), it's northbound
        if first_stop == '9769':
            return "1875876"
        # If first stop is NEWMARKET (9809), it's southbound
        elif first_stop == '9809':
            return "1875927"
    
    # Default to northbound if unable to determine
    logger.warning(f"Unable to determine SUMO route for route_id={route_id}, defaulting to northbound")
    return "1875876"

def create_bus_in_sumo(
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
):
    """
    Create a bus in SUMO and the corresponding SimPy Bus object.
    This function is kept as a utility function to be used by schedulers.
    """
    try:
        # Get SUMO route ID
        sumo_route_id = get_route_from_stops(route_stops, scenario_name, route_id)
        
        # Add vehicle to SUMO
        traci.vehicle.add(
            vehID=bus_id,
            routeID=sumo_route_id,
            typeID="bus",
            depart="now"
        )
        
        logger.info(f"Bus {bus_id} created, using predefined SUMO route stops")
        
        # Set bus capacity in SUMO if API available
        try:
            if hasattr(traci.vehicle, 'setPersonCapacity'):
                traci.vehicle.setPersonCapacity(bus_id, BUS_CAPACITY)
        except Exception:
            # Older libsumo may not support this; ignore if not available
            logger.debug("setPersonCapacity not supported by current libsumo version - skipping")
        
        # Create SimPy Bus object
        bus = Bus(
            env, bus_id, route_id, route_stops, bus_stops, event_handler,
            start_time=env.now,
            stop_mapping=stop_mapping,
            trip_data=trip_data
        )
        active_buses_list.append(bus)
        
        # Build subscription list based on available constants
        sub_vars = [
            traci.constants.VAR_ROAD_ID,
            traci.constants.VAR_POSITION,
            traci.constants.VAR_SPEED,
        ]
        if hasattr(traci.constants, 'VAR_STOP_STATE'):
            sub_vars.append(traci.constants.VAR_STOP_STATE)
        traci.vehicle.subscribe(bus_id, sub_vars)
        
        # Output bus dispatch information
        logger.info(f"{'='*80}")
        logger.info(f"BUS DISPATCHED: {bus_id}")
        logger.info(f"  Time: {env.now:.1f}s ({format_time(env.now)})")
        logger.info(f"  Trip ID: {trip_data.get('trip_id', 'N/A')}")
        logger.info(f"  Route: {route_id}")
        logger.info(f"  SUMO Route: {sumo_route_id}")
        logger.info(f"  Total Stops: {len(route_stops)}")
        if trip_data.get('stops'):
            logger.info(f"  First Stop: {route_stops[0]} at {format_time(trip_data['stops'][0]['scheduled_time'])}")
            logger.info(f"  Last Stop: {route_stops[-1]} at {format_time(trip_data['stops'][-1]['scheduled_time'])}")
        logger.info(f"  Capacity: {BUS_CAPACITY}")
        logger.info(f"{'='*80}")
        
        # Start the bus process
        env.process(bus.run())
        
        # Record dispatch event
        event_handler.record_event("bus_dispatched_from_timetable", {
            "bus_id": bus_id,
            "trip_id": trip_data.get('trip_id'),
            "route_id": route_id,
            "sumo_route": sumo_route_id,
            "scheduled_start": trip_data['departure_time']
        })
        
        # Return the Bus object
        return bus
        
    except traci.TraCIException as e:
        logger.error(f"Failed to create bus {bus_id} in SUMO: {e}")
        return None


def create_scheduler(
    env: simpy.Environment,
    scheduler_type: str = "timetable",
    **kwargs
) -> SchedulerInterface:
    """
    Creates a scheduler instance.
    
    Args:
        env: The SimPy environment.
        scheduler_type: The type of scheduler ("timetable", "interval", "adaptive", or "adaptive_headway").
        **kwargs: Scheduler-specific parameters.
        
    Returns:
        A scheduler instance.
    """
    if scheduler_type == "timetable":
        timetable_manager = kwargs.get('timetable_manager')
        if not timetable_manager:
            timetable_manager = TimetableManager()
        scheduler = TimetableScheduler(env, timetable_manager)
    elif scheduler_type == "interval":
        interval = kwargs.get('interval', DISPATCH_INTERVAL_SECONDS)
        scheduler = IntervalScheduler(env, default_interval=interval)
    elif scheduler_type == "adaptive":
        # Create an adaptive scheduler
        timetable_manager = kwargs.get('timetable_manager')
        if not timetable_manager:
            timetable_manager = TimetableManager()
        config = kwargs.get('config', {})
        adaptive_config = config.get('simpy', {}).get('scheduler', {}).get('adaptive', {})
        scheduler = AdaptiveScheduler(env, timetable_manager, adaptive_config)
        
        # If an event_handler is provided, set it as the data collector
        event_handler = kwargs.get('event_handler')
        if event_handler and hasattr(event_handler, 'enable_data_collection'):
            scheduler.set_data_collector(event_handler)
            logger.info("Adaptive scheduler connected to event handler's data collection feature")
    elif scheduler_type == "adaptive_headway":
        # Create an adaptive headway based scheduler
        config = kwargs.get('config', {})
        adaptive_headway_config = config.get('simpy', {}).get('scheduler', {}).get('adaptive_headway', {})
        # Ensure env is passed to the config
        adaptive_headway_config['env'] = env
        route_id = kwargs.get('route_id', '')
        bus_ids = kwargs.get('bus_ids', [])
        arrival_rates_file = kwargs.get('arrival_rates_file')
        
        scheduler = AdaptiveHeadwayScheduler(
            config=adaptive_headway_config,
            route_id=route_id,
            bus_ids=bus_ids,
            arrival_rates_file=arrival_rates_file
        )
        
        # If an event_handler is provided, it can be used to log events
        event_handler = kwargs.get('event_handler')
        if event_handler:
            scheduler.event_handler = event_handler
            logger.info("Adaptive headway based scheduler created")
    else:
        raise ValueError(f"Unknown scheduler type: {scheduler_type}")
        
    # Note: Do not set bus_creator here; let the caller set it at the appropriate time
    # scheduler.set_bus_creator(create_bus_in_sumo)
    
    return scheduler


def schedule_bus_dispatch(
    env: simpy.Environment,
    route_id: str,
    route_stops: List[str],
    interval: float,
    bus_stops: Dict[str, BusStop],
    active_buses_list: List[Bus],
    event_handler: 'EventHandler',
    timetable: Optional[List[Dict[str, Any]]] = None,
    stop_mapping: Dict[str, str] = None,
    config: Optional[Dict[str, Any]] = None,
    timetable_manager: Optional[TimetableManager] = None
):
    """
    Schedule bus dispatch based on timetable or interval.
    This function is kept for backward compatibility.
    
    Args:
        env: The SimPy environment.
        route_id: The route ID.
        route_stops: A list of stops on the route.
        interval: The default dispatch interval.
        bus_stops: A dictionary of BusStop objects.
        active_buses_list: A list of active buses.
        event_handler: The event handler.
        timetable: Timetable data (optional).
        stop_mapping: Stop ID mapping.
        config: Configuration dictionary (optional).
        timetable_manager: Timetable manager (optional).
    """
    global bus_counters
    
    # Initialize counter for this route
    if route_id not in bus_counters:
        bus_counters[route_id] = itertools.count(1)
    
    # Read scheduler type from config
    scheduler_type = "auto"  # Default to auto-selection
    if config and 'simpy' in config and 'scheduler' in config['simpy']:
        scheduler_type = config['simpy']['scheduler'].get('type', 'auto')
    
    # Decide which scheduler to use based on config and timetable availability
    if scheduler_type == "timetable" or (scheduler_type == "auto" and timetable is not None):
        # Use the timetable scheduler
        if not timetable_manager:
            timetable_manager = TimetableManager()
        if timetable is not None:
            timetable_manager.update_schedule(route_id, timetable)
        else:
            # Try to load the default timetable
            timetable_manager.load_timetable_from_file(TIMETABLE_FILE)
        
        scheduler = create_scheduler(env, "timetable", timetable_manager=timetable_manager)
        logger.info(f"Using TimetableScheduler for route {route_id}")
        
    elif scheduler_type == "interval" or (scheduler_type == "auto" and timetable is None):
        # Use the interval scheduler
        # Read interval settings from config
        if config and 'simpy' in config and 'scheduler' in config['simpy'] and 'interval' in config['simpy']['scheduler']:
            interval_config = config['simpy']['scheduler']['interval']
            interval = interval_config.get('default_interval', interval)
        
        scheduler = create_scheduler(env, "interval", interval=interval)
        logger.info(f"Using IntervalScheduler for route {route_id} with interval {interval}s")
        
    elif scheduler_type == "adaptive":
        # Use the adaptive scheduler
        if not timetable_manager:
            timetable_manager = TimetableManager()
        if timetable is not None:
            timetable_manager.update_schedule(route_id, timetable)
        else:
            # Try to load the default timetable
            timetable_manager.load_timetable_from_file(TIMETABLE_FILE)
        
        scheduler = create_scheduler(
            env, 
            "adaptive", 
            timetable_manager=timetable_manager,
            config=config,
            event_handler=event_handler
        )
        logger.info(f"Using AdaptiveScheduler for route {route_id}")
        
    elif scheduler_type == "adaptive_headway":
        # Use the adaptive headway based scheduler
        # Get the scenario name
        scenario_name = route_id.split('_')[0] if '_' in route_id else '601'
        
        # Build the path to the arrival rates file
        arrival_rates_file = config['paths']['simpy_data']['arrival_rates_file'].format(scenario_name=scenario_name)
        
        # Generate a list of available bus IDs for this route
        bus_ids = [f"{route_id}_bus_{i}" for i in range(1, 21)]  # Reserve 20 buses
        
        scheduler = create_scheduler(
            env,
            "adaptive_headway",
            config=config,
            route_id=route_id,
            bus_ids=bus_ids,
            arrival_rates_file=arrival_rates_file,
            event_handler=event_handler
        )
        
        # Initialize the stop sequence
        scheduler.initialize(route_stops)
        
        logger.info(f"Using AdaptiveHeadwayScheduler for route {route_id}")
        
    else:
        raise ValueError(f"Unknown scheduler type in config: {scheduler_type}")
    
    # Set the bus creation function (note: this happens after monkey patching)
    scheduler.set_bus_creator(create_bus_in_sumo)
    
    # Get the scenario name
    scenario_name = route_id.split('_')[0] if '_' in route_id else '601'
    
    # Set the active scheduler (for Holding strategy, etc.)
    from .simulation_runner import set_active_scheduler
    set_active_scheduler(scheduler)
    
    # Start scheduling
    scheduler.schedule_route(
        route_id=route_id,
        route_stops=route_stops,
        bus_stops=bus_stops,
        active_buses_list=active_buses_list,
        event_handler=event_handler,
        stop_mapping=stop_mapping,
        scenario_name=scenario_name
    )
    
    # Make the function a generator: wait until the simulation ends
    # This empty yield statement makes the function a generator, satisfying SimPy env.process()
    yield env.timeout(0)

# --- Interface Definition --- 
# - Main simulation should start one `schedule_bus_dispatch`