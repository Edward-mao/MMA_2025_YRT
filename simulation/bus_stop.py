import simpy
import random
import numpy as np
from typing import List, Dict, Any
import logging
import os
import sys

# Ensure script can find simulation package
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from .passenger import Passenger
try:
    from .config import (
        DISABLED_PASSENGER_PROBABILITY,
        REQUEUE_PROPORTION
    )
except ImportError:
    from config import (
        DISABLED_PASSENGER_PROBABILITY,
        REQUEUE_PROPORTION
    )

from .data_loader import (
    get_arrival_rate, get_destination_weights
)

try:
    from .simulation_utils import get_current_period, format_time, weighted_sampling
except ImportError:
    from simulation_utils import get_current_period, format_time, weighted_sampling

from .logger_config import get_logger, get_file_only_logger

# Standard logger for general messages (console and file)
logger = get_logger(__name__)
# Special logger for passenger generation (file only)
passenger_generation_logger = get_file_only_logger(__name__)

class BusStop:
    """Represents a bus stop in the simulation."""
    def __init__(self, env: simpy.Environment, stop_id: str, all_stop_ids: List[str],
                 routes_data: Dict[str, List[str]],
                 arrival_data: Dict, weight_data: Dict,
                 selected_month: int, selected_day: int,
                 simulation_start_time: float = 0):
        """
        Initializes a BusStop.

        Args:
            env: The SimPy environment.
            stop_id: The unique identifier for this bus stop.
            all_stop_ids: A list of all valid stop IDs in the system.
            routes_data: Dictionary mapping route_id to list of stop_ids for all routes.
            arrival_data: Dictionary containing passenger arrival rates.
            weight_data: Dictionary containing destination weight vectors (route-specific).
            selected_month: The randomly chosen month for the simulation run.
            selected_day: The randomly chosen day for the simulation run.
            simulation_start_time: The start time of the simulation.
        """
        self.env = env
        self.stop_id = stop_id
        self.all_stop_ids = all_stop_ids
        self.routes_data = routes_data
        self.arrival_data = arrival_data
        self.weight_data = weight_data
        self.selected_month = selected_month
        self.selected_day = selected_day
        # Ensure passenger generation aligns with the configured simulation start time
        self.simulation_start_time = simulation_start_time

        # Determine routes serving this stop
        self.serving_routes = [route_id for route_id, stops in routes_data.items() if stop_id in stops]

        # Use a SimPy Store to manage waiting passengers (FIFO queue)
        self.waiting_passengers = simpy.Store(env)

        # Start the passenger generation process for this stop
        self.action = env.process(self.generate_passengers())

    def generate_passengers(self):
        """SimPy process to generate passengers arriving at this stop according to Poisson process."""
        # Ensure we do not generate passengers before the global simulation start time
        if self.env.now < self.simulation_start_time:
            # Wait exactly until the configured start time before beginning generation
            yield self.env.timeout(self.simulation_start_time - self.env.now)

        while True:
            # 1. Determine current time details
            seconds_into_day = self.env.now % 86400
            # New constraint: do not generate passengers from 0:00 to 5:50 (21000 seconds)
            if seconds_into_day < 21000:
                # Wait until 05:50 to continue the loop
                yield self.env.timeout(21000 - seconds_into_day)
                continue
            period = get_current_period(seconds_into_day)

            if period is None:
                current_time_str = format_time(self.env.now)
                logger.warning(f"Warning @ {current_time_str}: Could not determine time period for hour {int(seconds_into_day / 3600)} at stop {self.stop_id}. Waiting 60s.")
                yield self.env.timeout(60) 
                continue

            # Log the exact period key being used
            logger.debug(f"Stop {self.stop_id}: Using period key '{period}' for hour {int(seconds_into_day / 3600)}.")

            # 2. Get arrival rate (lambda) for the current time
            # If a stop serves multiple routes, this model simplifies by using the first route's arrival data.
            # A more complex simulation might create a generator per route or sum their arrival rates.
            if not self.serving_routes:
                logger.warning(f"Stop {self.stop_id} has no serving routes. No passengers will be generated.")
                yield self.env.timeout(3600)  # Wait for an hour before checking again
                continue

            route_for_rate_lookup = self.serving_routes[0]
            lambda_ = get_arrival_rate(
                self.arrival_data, route_for_rate_lookup, self.stop_id, 
                self.selected_month, self.selected_day, period
            )

            if lambda_ is None or lambda_ <= 0:
                wait_duration = 3600 # Wait 1 hour
                yield self.env.timeout(wait_duration) 
                continue

            # 3. Calculate inter-arrival time
            lambda_per_second = lambda_ / 3600.0
            inter_arrival_time = random.expovariate(lambda_per_second)

            # 4. Wait for the calculated time
            yield self.env.timeout(inter_arrival_time)
            arrival_event_time_str = format_time(self.env.now)
            
            # 5. Determine VALID Intended Route
            valid_origin_routes = []
            for r_id in self.serving_routes:
                # Check if this route is defined in weight_data
                if r_id in self.weight_data:
                    # Check if this stop has subsequent stops on this route
                    route_stops = self.routes_data.get(r_id, [])
                    try:
                        current_stop_index = route_stops.index(self.stop_id)
                        if current_stop_index < len(route_stops) - 1: # Ensure it's not the last stop
                            valid_origin_routes.append(r_id)
                    except ValueError:
                        # This stop, despite being in serving_routes, isn't actually on the route list in routes_data.
                        # This indicates a data inconsistency, but we'll skip this route for generation.
                        # print(f"Debug: Data inconsistency? Stop '{self.stop_id}' in serving_routes for '{r_id}' but not in routes_data list: {route_stops}")
                        pass 
            
            if not valid_origin_routes:
                # No routes originate from this stop with defined weights OR it's the terminal stop for all serving routes
                # print(f"Debug @ {arrival_event_time_str}: No valid originating routes with weights/subsequent stops found for {self.stop_id}. Serving: {self.serving_routes}. Skipping passenger generation.")
                # Don't yield here, just continue the loop to wait for the next arrival time calculation
                continue 
                
            # Choose randomly from the routes that *can* originate from here according to weights
            intended_route_id = random.choice(valid_origin_routes)
            
            # 6. Generate the passenger base info
            is_disabled = random.random() < DISABLED_PASSENGER_PROBABILITY
            
            # 7. Assign Destination based on Intended Route
            destination = self.assign_destination(intended_route_id)
            
            if destination is None:
                 # assign_destination handles its own warnings/errors if weights are missing or no subsequent stops
                 # print(f"Debug @ {arrival_event_time_str}: assign_destination returned None for Route {intended_route_id} at stop {self.stop_id}. Skipping.")
                 continue 

            # 8. Create Passenger object
            new_passenger = Passenger(
                env=self.env,
                arrival_time_at_stop=self.env.now,
                origin_stop_id=self.stop_id,
                destination_stop_id=destination,
                intended_route_id=intended_route_id, # Pass the route ID
                is_disabled=is_disabled
            )
            
            # 9. Add passenger to the waiting queue
            passenger_generation_logger.info(f"Time {arrival_event_time_str}: Generated {new_passenger} at Stop {self.stop_id}")
            yield self.waiting_passengers.put(new_passenger)

    def assign_destination(self, route_id: str) -> str | None:
        """Assigns a destination stop based on the route, time, and weight vector."""
        # Get the sequence of stops for the intended route
        route_stops = self.routes_data.get(route_id)
        if not route_stops or len(route_stops) < 1:
            logger.error(f"Route ID '{route_id}' not found or empty in routes_data for stop {self.stop_id}.")
            return None
        
        # Find the index of the current stop on this route
        try:
            current_stop_index = route_stops.index(self.stop_id)
        except ValueError:
            logger.error(f"Current stop '{self.stop_id}' not found on intended route '{route_id}'.")
            return None
            
        # Determine subsequent stops on this route
        subsequent_stops = route_stops[current_stop_index + 1:]
        
        if not subsequent_stops:
            # Passenger generated at the terminal stop. No subsequent destination possible.
            logger.debug(f"Info @ {format_time(self.env.now)}: Passenger generated at terminal stop '{self.stop_id}' for route '{route_id}'. No subsequent stops.")
            return None 
        
        # --- Get weights for subsequent stops ---
        seconds_into_day = self.env.now % 86400
        period = get_current_period(seconds_into_day)
        if not period:
             logger.warning(f"Could not determine period for hour {int(seconds_into_day / 3600)} at stop {self.stop_id}. Using random fallback.")
             return random.choice(subsequent_stops)

        route_weights = get_destination_weights(self.weight_data, route_id, self.selected_month, self.selected_day, period)
        
        # Check if weights were found
        if route_weights is None:
            logger.warning(f"Could not get weights for route {route_id} at stop {self.stop_id}. Using random fallback.")
            return random.choice(subsequent_stops)
        
        # Validate weights length
        if len(route_weights) != len(self.routes_data[route_id]):
            logger.warning(f"Weights length ({len(route_weights)}) doesn't match route stops length ({len(self.routes_data[route_id])}) for route {route_id}.")
            return random.choice(subsequent_stops)
        
        # Extract weights for subsequent stops only
        subsequent_weights = route_weights[current_stop_index + 1:]
        
        if len(subsequent_weights) == 0:
            logger.warning(f"Warning: No subsequent weights available for route {route_id} at stop {self.stop_id}.")
            return None
        
        # --- Check for length mismatch --- 
        if len(subsequent_weights) != len(subsequent_stops):
            logger.warning(f"Warning: Mismatched weights length for route {route_id} at stop {self.stop_id}. Weights: {len(subsequent_weights)}, subsequent stops: {len(subsequent_stops)}. Using random fallback.")
            return random.choice(subsequent_stops) 

        # Convert weights to probability distribution
        total_weight = sum(subsequent_weights)
        if total_weight <= 0:
            logger.warning(f"Warning: Total weight is zero or negative for route {route_id} at stop {self.stop_id}. Using uniform random selection.")
            return random.choice(subsequent_stops)
        
        probabilities = [w / total_weight for w in subsequent_weights]
        
        # Use weighted sampling with probabilities
        chosen_destination = weighted_sampling(probabilities, subsequent_stops)
        
        return chosen_destination

    def add_passenger_to_queue(self, passenger: Passenger):
        """Allows adding a passenger externally (e.g., from interchange)."""
        # This method provides an interface for re-queuing
        logger.info(f"Time {self.env.now:.2f}: Re-queuing {passenger} at Stop {self.stop_id}. Queue size: {len(self.waiting_passengers.items) + 1}")
        self.env.process(self._put_passenger(passenger))
        
    def _put_passenger(self, passenger: Passenger):
        """Helper process to put passenger without blocking external caller."""
        yield self.waiting_passengers.put(passenger)
    
    def handle_bus_arrival(self, bus):
        """
        Handles the bus arrival event. This method is triggered by a SUMO event, not by a timetable.
        This method processes passenger alighting and boarding in parallel and calculates a variable dwell time based on the longer of the two.
        
        Args:
            bus: The arriving Bus object.
            
        Returns:
            A tuple (alighted_passengers, boarded_passengers, total_dwell_time)
        """
        arrival_time_str = format_time(self.env.now)
        logger.info(f"Time {arrival_time_str}: Bus {bus.bus_id} arrived at Stop {self.stop_id} (SUMO-triggered). Load: {bus.current_capacity_load}/{bus.bus_capacity} ({bus.disabled_passenger_count} disabled)")
        
        is_final_stop = (bus.current_stop_index == len(bus.route_stops) - 1)
        
        start_time = self.env.now
        
        # Process alighting and boarding in parallel
        alighting_proc = self.env.process(self._handle_alighting(bus, is_final_stop))
        
        if not is_final_stop:
            boarding_proc = self.env.process(self._handle_boarding(bus))
            # Wait for both processes to complete
            results = yield alighting_proc & boarding_proc
            
            alighted_count, _ = results[alighting_proc]
            boarded_count, _ = results[boarding_proc]
        else:
            # Only process alighting
            alighted_count, _ = yield alighting_proc
            boarded_count = 0
            logger.debug(f"Time {arrival_time_str}: No boarding at final stop {self.stop_id}")

        # The actual time passed for parallel processes is the time of the longest one
        parallel_process_time = self.env.now - start_time
        
        # Total dwell time
        total_dwell_time = parallel_process_time
        
        logger.info(f"Stop process for bus {bus.bus_id} at {self.stop_id} complete. Alighted: {alighted_count}, Boarded: {boarded_count}, Total dwell time: {total_dwell_time:.1f}s")

        return alighted_count, boarded_count, total_dwell_time
    
    def _handle_alighting(self, bus, is_final_stop: bool):
        """Processes passenger alighting and simulates the required time."""
        alighted_count = 0
        total_alighting_time = 0
        passengers_staying = []
        
        passengers_to_alight = []
        for passenger in bus.passengers:
            if is_final_stop or passenger.destination_stop_id == self.stop_id:
                passengers_to_alight.append(passenger)
            else:
                passengers_staying.append(passenger)
        
        bus.passengers = passengers_staying
        
        if passengers_to_alight:
            # Assume passengers alight one by one
            for passenger in passengers_to_alight:
                reason = "final stop" if is_final_stop else "destination"
                logger.debug(f"Time {format_time(self.env.now)}: {passenger} alighting at {self.stop_id} (Reason: {reason})")
                bus.current_capacity_load -= passenger.capacity_cost
                if passenger.is_disabled:
                    bus.disabled_passenger_count -= 1
                
                # Use the passenger's specific alighting time
                alighting_time = passenger.alighting_time
                yield self.env.timeout(alighting_time)
                total_alighting_time += alighting_time
                alighted_count += 1
                
                if hasattr(bus, 'event_handler') and bus.event_handler:
                    bus.event_handler.record_event('passenger_alighted', {
                        'bus_id': bus.bus_id,
                        'passenger_id': passenger.id,
                        'stop_id': self.stop_id,
                        'reason': reason
                    })
        
        return alighted_count, total_alighting_time
    
    def _handle_boarding(self, bus):
        """Handles passenger boarding."""
        # Get valid subsequent stops
        current_stop_index = bus.route_stops.index(self.stop_id)
        valid_future_stops = bus.route_stops[current_stop_index + 1:]
        
        if not valid_future_stops:
            logger.debug(f"No future stops for boarding at {self.stop_id}")
            yield self.env.timeout(0)
            return 0, 0
        
        boarded_count = 0
        total_boarding_time = 0
        initial_waiting_count = len(self.waiting_passengers.items)
        passengers_to_requeue = []
        
        logger.debug(f"Time {format_time(self.env.now)}: Attempting boarding at {self.stop_id}. Waiting: {initial_waiting_count}, Valid destinations: {valid_future_stops}")
        
        # Process waiting passengers
        processed_count = 0
        while processed_count < initial_waiting_count:
            # Check if the bus is full
            if bus.current_capacity_load >= bus.bus_capacity:
                logger.debug(f"Bus {bus.bus_id} full at {self.stop_id}")
                break
            
            if not self.waiting_passengers.items:
                break
            
            try:
                passenger = yield self.waiting_passengers.get()
                processed_count += 1
            except:
                break
            
            # Check if passenger's destination is among the subsequent stops
            if passenger.destination_stop_id not in valid_future_stops:
                logger.debug(f"{passenger} destination {passenger.destination_stop_id} not on remaining route")
                passengers_to_requeue.append(passenger)
                continue
            
            # Check capacity constraints
            can_board = False
            if passenger.is_disabled:
                if (bus.disabled_passenger_count < bus.max_disabled_passengers and
                    bus.current_capacity_load + passenger.capacity_cost <= bus.bus_capacity):
                    can_board = True
            else:
                if bus.current_capacity_load + passenger.capacity_cost <= bus.bus_capacity:
                    can_board = True
            
            if can_board:
                # Passenger boards
                bus.passengers.append(passenger)
                bus.current_capacity_load += passenger.capacity_cost
                if passenger.is_disabled:
                    bus.disabled_passenger_count += 1
                
                # Simulate boarding time
                boarding_time = passenger.boarding_time
                yield self.env.timeout(boarding_time)
                total_boarding_time += boarding_time
                boarded_count += 1
                
                logger.debug(f"{passenger} boarded Bus {bus.bus_id} at {self.stop_id}")
                
                # Record boarding event
                if hasattr(bus, 'event_handler') and bus.event_handler:
                    bus.event_handler.record_event('passenger_boarded', {
                        'bus_id': bus.bus_id,
                        'passenger_id': passenger.id,
                        'stop_id': self.stop_id,
                        'destination': passenger.destination_stop_id
                    })
            else:
                # Insufficient capacity, decide whether to requeue based on probability
                if random.random() < REQUEUE_PROPORTION:
                    passengers_to_requeue.append(passenger)
                    logger.debug(f"{passenger} denied boarding (capacity), requeuing")
                else:
                    logger.debug(f"{passenger} denied boarding (capacity), leaving")
        
        # Put passengers who need to requeue back into the queue
        for passenger in passengers_to_requeue:
            yield self.waiting_passengers.put(passenger)
        
        logger.info(f"Boarding process at {self.stop_id} complete. Boarded: {boarded_count}, Boarding time: {total_boarding_time:.1f}s")

        return boarded_count, total_boarding_time

# Interface Definition:
# - Initialization: Create BusStop(env, stop_id, all_stop_ids, routes_data, arrival_data, weight_data, selected_month, selected_day)
# - Passenger Access: Access waiting passengers via `bus_stop.waiting_passengers` (SimPy Store)
#   - `bus_stop.waiting_passengers.get()`: To retrieve a passenger (used by Bus)
#   - `bus_stop.waiting_passengers.items`: To see the current queue (list of passengers)
# - Re-queuing: Call `bus_stop.add_passenger_to_queue(passenger)` for interchange functionality.
# - Internal process `generate_passengers` runs automatically.
