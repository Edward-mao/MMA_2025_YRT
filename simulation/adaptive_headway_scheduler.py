"""
Adaptive Headway Scheduler implementation.
Dynamically adjusts bus headways based on predicted demand at specific monitored stops.
"""
from typing import Dict, List, Tuple, Optional, Set
from collections import deque
import logging
import json
from .scheduler_interface import SchedulerInterface
from .poisson_demand_predictor import PoissonDemandPredictor
from .bus_stop import BusStop
from .bus import Bus
from .event_handler import EventHandler
import simpy

logger = logging.getLogger(__name__)


class AdaptiveHeadwayScheduler(SchedulerInterface):
    """
    Adaptive scheduler based on fixed monitored stops.
    Once a bus is dispatched, its headway remains fixed throughout the trip.
    """
    
    # Define monitored stops for each route
    MONITORED_STOPS = {
        '601001': ['9769', '9770', '9723'],
        '601002': ['9819', '9883']
    }
    
    def __init__(self, config: dict, route_id: str, bus_ids: List[str], 
                 arrival_rates_file: str = None):
        """
        Initializes the adaptive headway scheduler.
        
        Args:
            config: Scheduler configuration.
            route_id: The route ID.
            bus_ids: A list of available bus IDs.
            arrival_rates_file: The path to the historical arrival rates file.
        """
        # Get env from config, or create a temporary one if not present
        env = config.get('env', simpy.Environment())
        super().__init__(env)
        
        self.route_id = route_id
        self.bus_ids = bus_ids
        
        # Extract configuration parameters
        self.beta_target = config.get('beta_target', 0.7)  # Target load factor
        self.h_min = config.get('h_min', 300)  # Minimum headway (seconds)
        self.h_max = config.get('h_max', 1800)  # Maximum headway (seconds)
        self.capacity = config.get('bus_capacity', 65)  # Vehicle capacity
        self.max_hold = config.get('max_hold', 60)  # Maximum hold time for headway control
        self.headway_tolerance = config.get('headway_tolerance', 0.2)  # 20% tolerance for bunching
        
        # Get monitored stops for this route
        self.monitored_stops = self.MONITORED_STOPS.get(route_id, [])
        
        # Initialize Poisson demand predictor
        self.demand_predictor = PoissonDemandPredictor(route_id=route_id)
        
        # Load historical data
        if arrival_rates_file:
            self.demand_predictor.load_historical_data(arrival_rates_file)
            
        # State management
        self.active_buses: Dict[str, Dict] = {}  # State of active vehicles
        self.last_departure_time = None
        self.stop_sequence: List[str] = []  # Stop sequence
        self.event_handler = None  # Will be set in schedule_route
        
        # KPI tracking
        self.kpi_data = {
            'headway_adjustments': [],
            'demand_predictions': [],
            'capacity_utilization': []
        }
        
        # Schedule process reference
        self.route_schedules = {}
        
    def initialize(self, stop_sequence: List[str]):
        """
        Initializes the stop sequence.
        """
        self.stop_sequence = stop_sequence
        logger.info(f"Initialized adaptive headway scheduler with {len(stop_sequence)} stops")
        logger.info(f"Monitoring stops for route {self.route_id}: {self.monitored_stops}")
        
    def schedule_route(
        self, 
        route_id: str,
        route_stops: List[str],
        bus_stops: Dict[str, BusStop],
        active_buses_list: List[Bus],
        event_handler: EventHandler,
        stop_mapping: Dict[str, str],
        **kwargs
    ):
        """
        Starts adaptive headway scheduling for the specified route.
        """
        # Save necessary references
        self.event_handler = event_handler
        self.stop_sequence = route_stops
        
        # Start the scheduling process
        schedule_process = self.env.process(
            self._adaptive_headway_schedule(
                route_id, route_stops, bus_stops, 
                active_buses_list, event_handler, stop_mapping, **kwargs
            )
        )
        self.route_schedules[route_id] = schedule_process
        
    def _adaptive_headway_schedule(
        self, 
        route_id: str,
        route_stops: List[str],
        bus_stops: Dict[str, BusStop],
        active_buses_list: List[Bus],
        event_handler: EventHandler,
        stop_mapping: Dict[str, str],
        **kwargs
    ):
        """Main loop for adaptive headway scheduling."""
        scenario_name = kwargs.get('scenario_name', '601')
        bus_counter = 1
        
        logger.info(f"{'='*80}")
        logger.info(f"Starting Adaptive Headway Scheduler")
        logger.info(f"  Route: {route_id}")
        logger.info(f"  Number of stops: {len(route_stops)}")
        logger.info(f"  Monitored stops: {self.monitored_stops}")
        logger.info(f"  Configuration parameters:")
        logger.info(f"    - β* (target load factor): {self.beta_target}")
        logger.info(f"    - h_min (minimum headway): {self.h_min}s")
        logger.info(f"    - h_max (maximum headway): {self.h_max}s")
        logger.info(f"    - Vehicle capacity: {self.capacity}")
        logger.info(f"{'='*80}")
        
        while True:
            # Calculate the next departure time
            result = self.schedule_next_departure(self.env.now)
            if not result:
                # No available vehicles, wait for a while and retry
                yield self.env.timeout(60)
                continue
                
            bus_id, departure_time, planned_headway = result
            
            # Wait until the departure time
            wait_time = departure_time - self.env.now
            if wait_time > 0:
                yield self.env.timeout(wait_time)
                
            # Create trip data with fixed headway
            trip_data = {
                'trip_id': f'adaptive_{bus_counter}',
                'departure_time': self.env.now,
                'stops': [{'stop_id': stop, 'scheduled_time': self.env.now} for stop in route_stops],
                'planned_headway': planned_headway,  # This will remain fixed for the entire trip
                'fixed_headway': True  # Flag to indicate fixed headway mode
            }
            
            # Create the bus
            if hasattr(self, 'bus_creator'):
                actual_bus_id = f"bus_{route_id}_adaptive_{bus_counter}_{int(self.env.now)}"
                bus = self.bus_creator(
                    env=self.env,
                    bus_id=actual_bus_id,
                    route_id=route_id,
                    route_stops=route_stops,
                    bus_stops=bus_stops,
                    active_buses_list=active_buses_list,
                    event_handler=event_handler,
                    trip_data=trip_data,
                    stop_mapping=stop_mapping,
                    scenario_name=scenario_name
                )
                
                # Update active bus information
                if bus:
                    self.active_buses[actual_bus_id] = self.active_buses.pop(bus_id, {})
                    self.active_buses[actual_bus_id]['bus_object'] = bus
                    self.active_buses[actual_bus_id]['fixed_headway'] = planned_headway
                    
            bus_counter += 1
            
            # Limit dispatch frequency to avoid rapid iteration
            if wait_time <= 0:
                yield self.env.timeout(1)
        
    def schedule_next_departure(self, current_time: float) -> Optional[Tuple[str, float, float]]:
        """
        Calculates the departure time for the next bus.
        
        Returns:
            (bus_id, departure_time, planned_headway) or None
        """
        # Get an available vehicle
        available_bus = self._get_available_bus()
        if not available_bus:
            return None
            
        # Calculate the target headway using the new formula
        target_headway = self._calculate_target_headway(current_time)
        
        # Determine the departure time
        if self.last_departure_time is None:
            departure_time = current_time
        else:
            departure_time = self.last_departure_time + target_headway
            
        # Ensure it is not earlier than the current time
        departure_time = max(departure_time, current_time)
        
        self.last_departure_time = departure_time
        
        # Record the active vehicle with fixed headway
        self.active_buses[available_bus] = {
            'departure_time': departure_time,
            'current_stop_index': 0,
            'remaining_capacity': self.capacity,
            'passengers_onboard': 0,
            'planned_headway': target_headway,
            'fixed_headway': target_headway  # This headway will not change
        }
        
        logger.info(f"Scheduled bus {available_bus} for departure at {departure_time:.0f}s "
                   f"with FIXED headway {target_headway:.0f}s")
        
        return available_bus, departure_time, target_headway
        
    def _calculate_target_headway(self, current_time: float) -> float:
        """
        Calculates the target headway using the new formula:
        h* = max(h_min, min(h_max, (β* × C) / (Σλ̂/n)))
        where n is the number of monitored stops
        """
        # If no monitored stops, use default minimum headway
        if not self.monitored_stops:
            logger.warning(f"No monitored stops defined for route {self.route_id}, using h_min")
            return self.h_min
            
        # Get demand predictions for monitored stops only
        total_demand_rate = 0.0
        n_monitored = len(self.monitored_stops)
        
        for stop_id in self.monitored_stops:
            # Predict demand for this stop using arrival rates
            demand_rate = self.demand_predictor.get_arrival_rate(stop_id, current_time)
            total_demand_rate += demand_rate
            
        # Calculate average demand rate
        avg_demand_rate = total_demand_rate / n_monitored if n_monitored > 0 else 0
        
        # Avoid division by zero
        if avg_demand_rate < 0.001:  # Very low demand
            target_headway = self.h_max
        else:
            # Apply the new formula: h* = (β* × C) / (Σλ̂/n)
            target_headway = (self.beta_target * self.capacity) / avg_demand_rate
            
        # Apply upper and lower limits
        target_headway = max(self.h_min, min(self.h_max, target_headway))
        
        # Record KPI
        self.kpi_data['demand_predictions'].append({
            'time': current_time,
            'monitored_stops': self.monitored_stops,
            'total_demand_rate': total_demand_rate,
            'avg_demand_rate': avg_demand_rate,
            'target_headway': target_headway
        })
        
        logger.debug(f"Calculated headway: {target_headway:.0f}s (avg demand rate: {avg_demand_rate:.3f})")
        
        return target_headway
        
    def update_on_stop_arrival(self, bus_id: str, stop_id: str, 
                              current_time: float, passengers_boarded: int = 0,
                              passengers_alighted: int = 0) -> Optional[float]:
        """
        Updates when a vehicle arrives at a stop.
        Only performs holding if the bus is running ahead of its fixed headway.
        
        Returns:
            hold_time (stop dwell time) or None
        """
        if bus_id not in self.active_buses:
            return None
            
        bus_state = self.active_buses[bus_id]
        
        # Update vehicle state
        bus_state['passengers_onboard'] += passengers_boarded - passengers_alighted
        bus_state['remaining_capacity'] = self.capacity - bus_state['passengers_onboard']
        
        # Get the current stop index
        try:
            current_index = self.stop_sequence.index(stop_id)
            bus_state['current_stop_index'] = current_index
        except ValueError:
            logger.warning(f"Stop {stop_id} not found in sequence")
            return None
            
        # Check if holding is needed to maintain fixed headway
        hold_time = self._calculate_hold_time_for_fixed_headway(bus_id, current_time)
        
        # Record capacity utilization
        utilization = bus_state['passengers_onboard'] / self.capacity
        self.kpi_data['capacity_utilization'].append({
            'time': current_time,
            'bus_id': bus_id,
            'utilization': utilization
        })
        
        return hold_time
        
    def _calculate_hold_time_for_fixed_headway(self, bus_id: str, current_time: float) -> Optional[float]:
        """
        Calculates hold time to maintain the fixed headway.
        Only holds if the bus is running ahead of schedule.
        """
        # Find the preceding bus
        preceding_bus = self._find_preceding_bus(bus_id)
        if not preceding_bus:
            return None
            
        bus_state = self.active_buses[bus_id]
        preceding_state = self.active_buses[preceding_bus]
        
        # Get the fixed headway for this bus
        fixed_headway = bus_state['fixed_headway']
        
        # Calculate actual time since preceding bus departure
        time_since_preceding = current_time - preceding_state['departure_time']
        
        # If running ahead of the fixed headway (within tolerance), calculate hold time
        # We want to maintain the headway, so if time_since_preceding < fixed_headway,
        # the bus is running ahead and needs to be held
        if time_since_preceding < fixed_headway:
            # Calculate how much to hold to reach the target headway
            hold_time = min(self.max_hold, fixed_headway - time_since_preceding)
            
            # Only hold if the deviation is significant (beyond tolerance)
            if hold_time > fixed_headway * self.headway_tolerance:
                # Too much holding needed, cap it
                hold_time = min(hold_time, self.max_hold)
            elif hold_time < fixed_headway * 0.05:  # Less than 5% deviation
                # Minor deviation, no holding needed
                return None
            
            # Record the adjustment
            self.kpi_data['headway_adjustments'].append({
                'time': current_time,
                'bus_id': bus_id,
                'fixed_headway': fixed_headway,
                'actual_time_gap': time_since_preceding,
                'hold_time': hold_time
            })
            
            logger.info(f"Bus {bus_id} holding for {hold_time:.0f}s to maintain fixed headway of {fixed_headway:.0f}s")
            
            return hold_time
            
        return None
        
    def _find_preceding_bus(self, bus_id: str) -> Optional[str]:
        """
        Finds the preceding bus on the same route.
        """
        bus_state = self.active_buses[bus_id]
        bus_departure = bus_state['departure_time']
        
        best_preceding = None
        latest_departure = -float('inf')
        
        for other_id, other_state in self.active_buses.items():
            if other_id == bus_id:
                continue
                
            # Find the bus that departed most recently before this one
            other_departure = other_state['departure_time']
            if other_departure < bus_departure and other_departure > latest_departure:
                latest_departure = other_departure
                best_preceding = other_id
                    
        return best_preceding
        
    def get_next_departure_time(self, route_id: str) -> Optional[float]:
        """
        Gets the next departure time (implements the interface method).
        """
        result = self.schedule_next_departure(self.env.now)
        if result:
            _, departure_time, _ = result
            return departure_time
        return None
        
    def update_schedule(self, route_id: str, schedule_data) -> bool:
        """
        Updates scheduling parameters (implements the interface method).
        """
        if isinstance(schedule_data, dict):
            # Update configuration parameters
            for key, value in schedule_data.items():
                if hasattr(self, key):
                    setattr(self, key, value)
                    logger.info(f"Updated {key} to {value}")
            return True
        return False
        
    def _get_available_bus(self) -> Optional[str]:
        """
        Gets an available vehicle.
        """
        for bus_id in self.bus_ids:
            if bus_id not in self.active_buses:
                return bus_id
        return None
        
    def remove_completed_bus(self, bus_id: str):
        """
        Removes a vehicle that has completed its operation.
        """
        if bus_id in self.active_buses:
            del self.active_buses[bus_id]
            logger.info(f"Removed completed bus {bus_id} from active roster")
            
    def export_kpi_data(self, output_file: str):
        """
        Exports KPI data.
        """
        try:
            with open(output_file, 'w') as f:
                json.dump(self.kpi_data, f, indent=2)
            logger.info(f"Exported KPI data to {output_file}")
        except Exception as e:
            logger.error(f"Failed to export KPI data: {e}")

    def on_bus_arrival(self, bus: Bus, stop_id: str) -> float:
        """
        Callback method for when a vehicle arrives at a stop, returns the required hold time.
        
        Args:
            bus: The Bus object.
            stop_id: The stop ID.
            
        Returns:
            hold_time: The additional time to hold at the stop (in seconds).
        """
        # Get the number of passengers alighting and boarding (read from the bus object)
        passengers_onboard = len(bus.passengers) if hasattr(bus, 'passengers') else 0
        passengers_boarded = getattr(bus, 'last_boarded_count', 0)
        passengers_alighted = getattr(bus, 'last_alighted_count', 0)
        
        # Call the update method
        hold_time = self.update_on_stop_arrival(
            bus.bus_id, 
            stop_id, 
            self.env.now, 
            passengers_boarded, 
            passengers_alighted
        )
        
        # If there is a hold_time, record the event
        if hold_time and hold_time > 0 and self.event_handler:
            self.event_handler.record_event('headway_adjust', {
                'bus_id': bus.bus_id,
                'stop_id': stop_id,
                'hold_time': hold_time,
                'time': self.env.now,
                'reason': 'maintaining_fixed_headway'
            })
            
        return hold_time or 0.0 