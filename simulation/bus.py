# simulation/bus.py
"""
Bus class representing a bus vehicle in the simulation.
"""
import simpy
import random
import math # For calculations
from typing import List, Dict, Optional, TYPE_CHECKING, Any

# Use relative imports for modules within the same package
from .passenger import Passenger
from .bus_stop import BusStop
from .config import (
    BUS_CAPACITY, MAX_DISABLED_PASSENGERS_PER_BUS, FIXED_DWELL_TIME,
    REQUEUE_PROPORTION, DISABLED_CAPACITY_COST, REGULAR_CAPACITY_COST,
    MEAN_TIME_BETWEEN_FAILURES,
    DEFAULT_DISTANCE_METERS, TRAVEL_TIME_FALLBACK_SPEED_FACTOR,
    MAX_BOARDING_ITERATIONS
)

from .simulation_utils import format_time
from .logger_config import get_logger, log_event, log_failure
from simulation.event_handler import EventHandler

# Use libsumo for performance - required for simulation
import libsumo as traci

# Type checking import to avoid circular dependency
if TYPE_CHECKING:
    from event_handler import EventHandler 

# Get logger for this module
logger = get_logger(__name__)

# -------------------------
#   Load vehicle dynamics
# -------------------------
# To ensure consistency between SUMO and SimPy, directly read accel/decel/maxSpeed
# for vType id="bus" from vtypes.xml. Use reasonable defaults if parsing fails.

import xml.etree.ElementTree as ET
from pathlib import Path

def _load_bus_dynamics(vtypes_path: Path) -> tuple[float, float, float]:
    """Parse vtypes.xml to obtain (accel, decel, max_speed) for vType id="bus"."""
    try:
        tree = ET.parse(vtypes_path)
        root = tree.getroot()
        bus_elem = root.find(".//vType[@id='bus']")
        if bus_elem is None:
            raise ValueError("No <vType id='bus'> found in vtypes.xml")

        accel = float(bus_elem.get("accel", "1.0"))
        decel = float(bus_elem.get("decel", "1.0"))
        max_speed = float(bus_elem.get("maxSpeed", "15"))
        return accel, decel, max_speed
    except Exception as e:
        logger.warning(f"Failed to parse {vtypes_path}: {e}. Using fallback dynamics.")
        return 1.0, 1.0, 15.0


# Locate vtypes.xml relative to project root
_project_root = Path(__file__).resolve().parents[1]
_vtypes_file = _project_root / "vtypes.xml"

_ACCEL, _DECEL, _MAX_SPEED = _load_bus_dynamics(_vtypes_file)

# Expose as module-level constants for compatibility with existing code
ACCELERATION = _ACCEL
DECELERATION = _DECEL
MAX_SPEED = _MAX_SPEED

logger.info(
    f"Loaded bus dynamics from vtypes.xml -> accel={ACCELERATION}, decel={DECELERATION}, maxSpeed={MAX_SPEED}")

# --- Helper function for travel time ---
def calculate_travel_time(distance: float, accel: float, decel: float, max_speed: float) -> float:
    """
    Calculates travel time for a given distance, considering acceleration,
    deceleration, and maximum speed. Assumes constant acceleration/deceleration phases.

    Args:
        distance: Distance to travel (meters).
        accel: Acceleration rate (m/s^2).
        decel: Deceleration rate (m/s^2).
        max_speed: Maximum cruising speed (m/s).

    Returns:
        Estimated travel time in seconds.
        
    Raises:
        ValueError: If any input parameters are invalid.
    """
    # Input validation
    if distance < 0:
        raise ValueError(f"Distance cannot be negative: {distance}")
    if distance == 0:
        return 0.0
    if accel <= 0:
        raise ValueError(f"Acceleration must be positive: {accel}")
    if decel <= 0:
        raise ValueError(f"Deceleration must be positive: {decel}")
    if max_speed <= 0:
        raise ValueError(f"Maximum speed must be positive: {max_speed}")

    try:
        # Time to reach max speed from zero
        time_to_accel = max_speed / accel
        # Distance covered during acceleration
        dist_accel = 0.5 * accel * time_to_accel**2

        # Time to stop from max speed
        time_to_decel = max_speed / decel
        # Distance covered during deceleration
        dist_decel = 0.5 * decel * time_to_decel**2

        # Check if the bus reaches max speed
        if dist_accel + dist_decel >= distance:
            # Bus does not reach max speed. It accelerates then immediately decelerates.
            # Calculate time for acceleration phase
            denominator = accel + (accel**2 / decel)
            if denominator <= 0:
                raise ValueError(f"Invalid acceleration/deceleration combination: accel={accel}, decel={decel}")
                
            t_accel = math.sqrt(2 * distance / denominator)
            t_decel = (accel / decel) * t_accel
            return t_accel + t_decel
        else:
            # Bus reaches max speed.
            # Distance covered at constant max speed
            dist_cruise = distance - dist_accel - dist_decel
            time_cruise = dist_cruise / max_speed
            return time_to_accel + time_cruise + time_to_decel
            
    except (ValueError, ZeroDivisionError, ArithmeticError) as e:
        # Log the error with full context
        error_msg = f"Error calculating travel time: distance={distance}, accel={accel}, decel={decel}, max_speed={max_speed}. Error: {str(e)}"
        logger.error(error_msg)
        # Use a reasonable fallback: average speed of max_speed/2
        fallback_time = distance / (max_speed / 2)
        logger.warning(f"Using fallback travel time: {fallback_time:.2f}s")
        return fallback_time

class Bus:
    """
    Represents a bus in the simulation, with both SimPy process logic
    and SUMO interaction capabilities.
    """
    def __init__(self, env: simpy.Environment, bus_id: str, route_id: str, 
                 route_stops: List[str], bus_stops: Dict[str, BusStop], 
                 event_handler: EventHandler, start_time: float,
                 stop_mapping: Dict[str, str], trip_data: Optional[Dict[str, Any]] = None):
        self.env = env
        self.bus_id = bus_id
        self.route_id = route_id
        self.route_stops = route_stops
        self.bus_stops = bus_stops
        self.event_handler = event_handler
        self.start_time = start_time
        
        # Store trip timetable data
        self.trip_data = trip_data or {}
        # Create a mapping from stop ID to scheduled arrival time
        self.stop_schedules = {}
        if 'stops' in self.trip_data:
            for stop_info in self.trip_data['stops']:
                self.stop_schedules[stop_info['stop_id']] = stop_info['scheduled_time']
        
        # Handle both new and old stop mapping formats
        if isinstance(stop_mapping, dict) and 'simpy_to_sumo' in stop_mapping:
            # New bidirectional mapping format
            # Determine direction based on route_id
            if route_id.endswith('001') or 'northbound' in route_id.lower():
                self.stop_mapping = stop_mapping['simpy_to_sumo'].get('northbound', {})
                self.direction = 'northbound'
            elif route_id.endswith('002') or 'southbound' in route_id.lower():
                self.stop_mapping = stop_mapping['simpy_to_sumo'].get('southbound', {})
                self.direction = 'southbound'
            else:
                # Default to northbound mapping
                logger.warning(f"Could not determine direction from route_id '{route_id}', defaulting to northbound mapping")
                self.stop_mapping = stop_mapping['simpy_to_sumo'].get('northbound', {})
                self.direction = 'northbound'
        else:
            # Old format, direct key-value mapping
            self.stop_mapping = stop_mapping
            self.direction = 'unknown'
        
        # Log the mapping being used
        logger.info(f"Bus {bus_id} (route {route_id}) initialized with {self.direction} stop mapping")
        
        self.current_stop_index = 0
        self.current_capacity_load = 0
        self.disabled_passenger_count = 0
        self.log: List[tuple] = []
        self.failed = False
        
        # State machine: Idle, EnRoute, Dwelling
        self.state = "Idle"
        logger.info(f"Bus {self.bus_id} created. Initial state: {self.state}")
        
        # SimPy event to signal passenger boarding is complete for a stop
        self.boarding_complete = self.env.event()
        
        # Add capacity-related attributes
        self.bus_capacity = BUS_CAPACITY  # Total capacity imported from config
        self.max_disabled_passengers = MAX_DISABLED_PASSENGERS_PER_BUS  # Max disabled passengers
        self.passengers = []  # Current passenger list
        
        # To track if a stop is currently being processed
        self.processing_stop = False
        self.processing_stop_id = None  # ID of the stop currently being processed
        self.stop_processing_complete = False  # Flag indicating if stop processing is complete (including index update)
        self.expected_arrival_time = None  # Expected arrival time (if any)

    def run(self):
        """Main process for the bus. This is now simplified as most of the
        movement logic is deferred to SUMO. The process waits for SUMO events."""
        self.state = "EnRoute"
        logger.info(f"Bus {self.bus_id} state changed to: {self.state}")
        self.log.append((self.env.now, f"Bus {self.bus_id} starts route {self.route_id}"))
        self.event_handler.record_event(self.env.now, "bus_start_route", {"bus_id": self.bus_id})
        
        # The run process now doesn't need to manage travel times,
        # it just needs to be kept alive to handle events triggered by the runner.
        # It will be terminated implicitly when the simulation ends.
        while self.state != "Finished":
             yield self.env.timeout(3600) # Wake up periodically to check status or wait for events.
        
    def handle_sumo_stop_arrival(self, stop_id: str):
        """
        Process triggered by the SimulationRunner when SUMO reports arrival at a stop.
        This method now properly handles early/late arrivals.
        """
        # Prevent duplicate processing of the same stop
        if self.processing_stop:
            logger.warning(f"Bus {self.bus_id} already processing stop, ignoring duplicate arrival event")
            return
        
        self.processing_stop = True
        self.processing_stop_id = stop_id
        self.stop_processing_complete = False  # Reset flag
        self.state = "Dwelling"
        logger.info(f"Bus {self.bus_id} state changed to: {self.state}")
        
        # Record the difference between actual and expected arrival time
        actual_arrival_time = self.env.now
        scheduled_arrival_time = self.stop_schedules.get(stop_id)
        time_diff_str = ""
        deviation = 0
        
        if scheduled_arrival_time is not None:
            deviation = actual_arrival_time - scheduled_arrival_time
            if abs(deviation) > 1:  # Ignore differences within 1 second
                if deviation > 0:
                    time_diff_str = f" (late by {deviation:.1f}s)"
                else:
                    time_diff_str = f" (early by {-deviation:.1f}s)"
        
        self.log.append((self.env.now, f"Arrived at stop {stop_id}{time_diff_str}"))
        logger.info(f"SUMO event: Bus {self.bus_id} arrived at stop {stop_id} at time {self.env.now:.1f}{time_diff_str}")
        
        # Record bus_arrival event, including scheduled_time
        if self.event_handler:
            event_details = {
                'bus_id': self.bus_id,
                'stop_id': stop_id,
                'time': actual_arrival_time,
                'load': self.current_capacity_load,
                'disabled_count': self.disabled_passenger_count
            }
            # Only add scheduled_time field if it exists
            if scheduled_arrival_time is not None:
                event_details['scheduled_time'] = scheduled_arrival_time
                event_details['deviation'] = deviation
            
            self.event_handler.record_event('bus_arrival', event_details)
        
        bus_stop = self.bus_stops.get(stop_id)
        if not bus_stop:
            self.log.append((self.env.now, f"Error: Stop {stop_id} not found in SimPy objects."))
            # If stop doesn't exist in SimPy, tell SUMO to continue immediately
            traci.vehicle.setStop(self.bus_id, stop_id, duration=0)
            self.state = "EnRoute"
            logger.info(f"Bus {self.bus_id} state changed to: {self.state} (Stop not found in SimPy)")
            self.processing_stop = False
            return
            
        # Start the coroutine to process the stop
        self.env.process(self._process_stop_arrival(bus_stop, stop_id))
    
    def _process_stop_arrival(self, bus_stop: BusStop, stop_id: str):
        """
        Coroutine to handle bus arrival at a stop, including passenger boarding/alighting and setting dwell time.
        """
        try:
            logger.info(f"Bus {self.bus_id} _process_stop_arrival START - current_stop_index: {self.current_stop_index}, stop: {stop_id}")
            
            # Check if Holding strategy needs to be executed
            hold_time = 0
            from .simulation_runner import get_active_scheduler
            scheduler = get_active_scheduler()
            if scheduler and hasattr(scheduler, 'on_bus_arrival'):
                hold_time = scheduler.on_bus_arrival(self, stop_id)
                if hold_time > 0:
                    logger.info(f"Bus {self.bus_id} executing Holding strategy, waiting at stop {stop_id} for {hold_time:.1f} seconds")
                    yield self.env.timeout(hold_time)
            
            # Call the stop's handler method
            alighted, boarded, dwell_time = yield self.env.process(
                bus_stop.handle_bus_arrival(self)
            )
            
            # Record the result
            self.log.append((self.env.now, 
                f"Stop processing complete: {alighted} alighted, {boarded} boarded, dwell time: {dwell_time:.1f}s"))
            
            # Set SUMO's dwell time
            try:
                # Use mapping to convert stop_id
                sumo_stop_id = self.stop_mapping.get(stop_id)
                if sumo_stop_id is None:
                    raise ValueError(f"Stop ID {stop_id} not found in stop_mapping.")

                # Dynamically override the dwell time for the current stop to ensure SUMO and SimPy are consistent
                try:
                    if hasattr(traci.vehicle, 'setBusStop'):
                        # Re-setting for the same stop can override the duration even if the vehicle has arrived
                        traci.vehicle.setBusStop(self.bus_id, sumo_stop_id, duration=dwell_time)
                except traci.TraCIException as e:
                    logger.warning(f"Failed to dynamically set dwell time for bus {self.bus_id} at stop {stop_id}: {e}")
                
                # Commented out route modification to avoid "Invalid route replacement" error
                # Let SUMO run according to the predefined route and stop sequence
                # try:
                #     # Get the vehicle's current route
                #     route = traci.vehicle.getRoute(self.bus_id)
                #     if route:
                #         # Keep the original route
                #         traci.vehicle.setRoute(self.bus_id, route)
                # except traci.TraCIException as e:
                #     logger.debug(f"Could not refresh route for bus {self.bus_id}: {e}")
                
                self.event_handler.record_event(self.env.now, "bus_set_dwell_time", 
                    {"bus_id": self.bus_id, "stop_id": stop_id, "duration": dwell_time})
            except (traci.TraCIException, ValueError) as e:
                self.log.append((self.env.now, f"Error setting dwell time in SUMO: {e}"))
                logger.error(f"Error setting dwell time for bus {self.bus_id}: {e}")
            
            # Wait for the dwell time in SimPy
            logger.info(f"Bus {self.bus_id} dwelling at stop {stop_id} for {dwell_time:.1f}s")
            yield self.env.timeout(dwell_time)
            
            # Update state, prepare for next stop
            self.state = "EnRoute"
            logger.info(f"Bus {self.bus_id} state changed to: {self.state}")
            
            # Mark processing as complete (before updating the index)
            self.stop_processing_complete = True
            
            # Important: Only update the index after stop processing is fully complete.
            # However, note that the bus may still be physically at the stop.
            # So we add a flag to indicate "waiting to leave stop".
            self._waiting_to_leave_stop = True
            self._completed_stop_id = stop_id
            
            if self.current_stop_index < len(self.route_stops) - 1:
                # Prepare to leave for the next stop, but don't update the index yet
                next_stop_index = self.current_stop_index + 1
                next_stop_id = self.route_stops[next_stop_index]
                logger.info(f"Bus {self.bus_id} ready to leave for next stop: {next_stop_id} (index will update after departure)")
                logger.info(f"Bus {self.bus_id} route_stops: {self.route_stops[:5]}... (showing first 5)")
            else:
                self.handle_route_end()

            # Return passenger boarding/alighting data to the caller (e.g., data collection extension class)
            return (alighted, boarded)
            
        except Exception as e:
            logger.error(f"Error processing stop arrival for bus {self.bus_id}: {e}", exc_info=True)
            self.state = "EnRoute"
            logger.info(f"Bus {self.bus_id} state changed to: {self.state} (Error during stop processing)")
            self.stop_processing_complete = True  # Mark as complete even if there's an error to avoid deadlock
        finally:
            self.processing_stop = False
            self.processing_stop_id = None

    def handle_boarding_complete(self, boarded_passengers: int, alighted_passengers: int, dwell_time: float):
        """
        Callback from BusStop's passenger_generator once boarding is done.
        Sets the dwell time in SUMO.
        """
        self.log.append((self.env.now, f"Boarding complete at {self.current_stop_id()}. {boarded_passengers} boarded, {alighted_passengers} alighted."))
        
        try:
            # Command SUMO to wait for the calculated dwell time
            current_simpy_stop_id = self.current_stop_id()
            sumo_stop_id = self.stop_mapping.get(current_simpy_stop_id)
            if sumo_stop_id is None:
                raise ValueError(f"Stop ID {current_simpy_stop_id} not found in stop mapping for bus {self.bus_id}")

            if hasattr(traci.vehicle, 'setBusStop'):
                traci.vehicle.setBusStop(self.bus_id, sumo_stop_id, duration=dwell_time)
                self.event_handler.record_event(self.env.now, "bus_set_dwell_time", {"bus_id": self.bus_id, "duration": dwell_time})
        except (traci.TraCIException, ValueError) as e:
            self.log.append((self.env.now, f"Error setting dwell time in SUMO for bus {self.bus_id}: {e}"))
            
        self.state = "EnRoute"
        logger.info(f"Bus {self.bus_id} state changed to: {self.state}")
        # Move to the next stop in the route sequence
        if self.current_stop_index < len(self.route_stops) - 1:
            self.current_stop_index += 1
        else:
            self.handle_route_end()

    def handle_route_end(self):
        """Called when the bus finishes its route."""
        self.state = "Finished"
        logger.info(f"Bus {self.bus_id} state changed to: {self.state}")
        self.log.append((self.env.now, f"Bus {self.bus_id} finished route."))
        self.event_handler.record_event(self.env.now, "bus_end_route", {"bus_id": self.bus_id})

    def current_stop_id(self) -> str:
        """Returns the current stop ID based on the index."""
        if self.current_stop_index < len(self.route_stops):
            return self.route_stops[self.current_stop_index]
        return "None"

    def get_current_stop_id(self) -> str:
        """Returns the ID of the current or next stop."""
        # Check bounds to prevent errors if called after route completion/failure
        if 0 <= self.current_stop_index < len(self.route_stops):
            return self.route_stops[self.current_stop_index]
        return "N/A" # Or some other indicator

    def monitor_failure(self):
        """SimPy process to randomly trigger a vehicle failure."""
        # Use exponential distribution for time between failures
        # Rate (lambda) for exponential is 1 / mean_time
        failure_rate = 1.0 / MEAN_TIME_BETWEEN_FAILURES if MEAN_TIME_BETWEEN_FAILURES > 0 else 0

        if failure_rate <= 0:
            # No failures configured or invalid MTBF
            yield self.env.timeout(float('inf')) # Wait forever
            return

        while not self.failed: # Only monitor if not already failed
            # Calculate time until the next potential failure event
            time_to_next_failure_check = random.expovariate(failure_rate)

            try:
                # Wait for that amount of time OR until the main process finishes/fails
                yield self.env.timeout(time_to_next_failure_check)

                # If timeout completes without interruption, trigger failure
                self.failed = True
                failure_time_str = format_time(self.env.now)
                log_failure(logger, f"Bus {self.bus_id} ** FAILED ** !!")
                self.log.append((failure_time_str, "failure", self.bus_id))

                # Interrupt the main run_route process
                if self.action.is_alive:
                    self.action.interrupt("Bus Failed")

                # Use event handler if available
                if self.event_handler:
                    self.event_handler.handle_bus_failure(
                        self, self.bus_stops, self.active_buses_list
                    )
                else:
                    logger.warning(f"No event handler available for bus failure of {self.bus_id}")

            except simpy.Interrupt:
                # Interrupted, likely because run_route finished normally
                logger.debug(f"Failure monitor for {self.bus_id} interrupted (likely normal finish).")
                break # Stop monitoring

    def run_route(self):
        """SimPy process for the bus operating along its route."""
        try:
            start_time_str = format_time(self.env.now)
            log_event(logger, f"Bus {self.bus_id} starting route at Stop {self.get_current_stop_id()}")
            self.log.append((start_time_str, "start_route", self.get_current_stop_id()))
            
            while self.current_stop_index < len(self.route_stops):
                current_stop_id = self.get_current_stop_id()
                current_bus_stop = self.bus_stops[current_stop_id]

                # --- Arrival Logic (occurs after potential travel timeout) ---
                arrival_time = self.env.now
                arrival_time_str = format_time(arrival_time)
                log_event(logger, f"Bus {self.bus_id} arrived at Stop {current_stop_id}. Load: {self.current_capacity_load}/{BUS_CAPACITY} ({self.disabled_passenger_count} disabled)")
                self.log.append((arrival_time_str, "arrive_stop", current_stop_id, self.current_capacity_load, self.disabled_passenger_count))
                
                # Record arrival event
                if self.event_handler:
                    self.event_handler.record_event('bus_arrived', {
                        'bus_id': self.bus_id,
                        'stop_id': current_stop_id,
                        'load': self.current_capacity_load,
                        'disabled_count': self.disabled_passenger_count
                    })

                # Check if this is the final stop in the current route direction
                is_final_stop_in_direction = (self.current_stop_index == len(self.route_stops) - 1)

                # --- Dwell Logic ---
                disembark_time = yield self.env.process(self.disembark_passengers(current_bus_stop, is_final_stop_in_direction))
                boarding_time = yield self.env.process(self.board_passengers(current_bus_stop))
                total_stop_time = FIXED_DWELL_TIME + disembark_time + boarding_time
                dwell_start_str = format_time(self.env.now)
                logger.debug(f"Bus {self.bus_id} dwelling at {current_stop_id} for {total_stop_time:.2f}s (Fixed: {FIXED_DWELL_TIME}, Disembark: {disembark_time:.2f}, Board: {boarding_time:.2f})")
                
                yield self.env.timeout(total_stop_time)
                depart_time_str = format_time(self.env.now)
                self.log.append((depart_time_str, "depart_stop", current_stop_id, self.current_capacity_load, self.disabled_passenger_count))
                log_event(logger, f"Bus {self.bus_id} departing Stop {current_stop_id}. New Load: {self.current_capacity_load}/{BUS_CAPACITY} ({self.disabled_passenger_count} disabled)")
                
                # Record departure event
                if self.event_handler:
                    self.event_handler.record_event('bus_departed', {
                        'bus_id': self.bus_id,
                        'stop_id': current_stop_id,
                        'load': self.current_capacity_load,
                        'disabled_count': self.disabled_passenger_count
                    })

                # --- Prepare for next leg or finish ---
                self.current_stop_index += 1

                if self.current_stop_index >= len(self.route_stops):
                    # End of route
                    end_time_str = format_time(self.env.now)
                    log_event(logger, f"Bus {self.bus_id} completed its route at {current_stop_id}. Final Load: {self.current_capacity_load}")
                    self.log.append((end_time_str, "end_route", current_stop_id))
                    break # Exit while loop

                else:
                    # --- Travel Time Calculation and Yield ---
                    next_stop_id = self.get_current_stop_id()
                    
                    # Use default distance for travel time calculation
                    distance = DEFAULT_DISTANCE_METERS
                    
                    try:
                        travel_time = calculate_travel_time(distance, ACCELERATION, DECELERATION, MAX_SPEED)
                    except ValueError as e:
                        logger.error(f"ERROR: Invalid parameters for travel time calculation: {e}")
                        # Use simple fallback calculation
                        travel_time = distance / (MAX_SPEED * TRAVEL_TIME_FALLBACK_SPEED_FACTOR)
                        logger.warning(f"Using fallback travel time: {travel_time:.2f}s")

                    logger.info(f"Time {depart_time_str}: Bus {self.bus_id} travelling from {current_stop_id} to {next_stop_id} ({distance}m, takes {travel_time:.2f}s)...\n")
                    
                    yield self.env.timeout(travel_time)
                    # Arrival message will be printed at the start of the next loop

        except simpy.Interrupt as interrupt:
            # Handle interruptions, e.g., bus failure
            if interrupt.cause == "Bus Failed":
                fail_time_str = format_time(self.env.now)
                logger.info(f"Bus {self.bus_id} process received failure interrupt. Stopping route.")
                # Log already handled in monitor_failure
            else:
                # Handle other potential interrupts if needed
                logger.info(f"Bus {self.bus_id} process interrupted: {interrupt.cause}")
        finally:
             # Ensure failure monitor is stopped if run_route exits for any reason
             if self.failure_monitor.is_alive:
                 self.failure_monitor.interrupt("Route Finished or Failed")


    def disembark_passengers(self, current_bus_stop: BusStop, is_final_stop: bool):
        """Process passengers getting off. If is_final_stop is True, all passengers disembark. (Generator)"""
        disembarking_passengers = []
        passengers_staying = []
        total_disembark_time = 0 # Assuming disembarking is quick/parallel for simplicity

        disembark_event_time_str = format_time(self.env.now) # Capture time before potential requeue
        for p in self.passengers:
            should_disembark = False
            reason = ""

            if is_final_stop:
                should_disembark = True
                reason = "final stop"
            elif p.destination_stop_id == current_bus_stop.stop_id:
                 should_disembark = True
                 reason = "destination"

            if should_disembark:
                logger.debug(f"Time {disembark_event_time_str}:   {p} disembarking at {current_bus_stop.stop_id} (Reason: {reason}).")
                self.log.append((disembark_event_time_str, f"disembark_{reason}", p.id, current_bus_stop.stop_id))
                self.current_capacity_load -= p.capacity_cost
                if p.is_disabled:
                    self.disabled_passenger_count -= 1
                
                # Record disembark event
                if self.event_handler:
                    self.event_handler.record_event('passenger_disembarked', {
                        'bus_id': self.bus_id,
                        'passenger_id': p.id,
                        'stop_id': current_bus_stop.stop_id,
                        'reason': reason
                    })

            else:
                passengers_staying.append(p)

        self.passengers = passengers_staying
        yield self.env.timeout(0) # Still need to yield to be a generator
        return total_disembark_time


    def board_passengers(self, current_bus_stop: BusStop):
        """Process passengers getting on the bus at the current stop. (Generator)"""
        board_start_time_str = format_time(self.env.now)
        total_boarding_time = 0
        boarded_count = 0
        skipped_invalid_dest = 0
        denied_capacity = 0
        processed_passengers_count = 0 # Counter to avoid infinite loops

        # --- Check 1: Is this the final stop? ---
        if self.current_stop_index == len(self.route_stops) - 1:
             logger.debug(f"Time {board_start_time_str}:   Boarding skipped at {current_bus_stop.stop_id} (Final stop of the route).")
             yield self.env.timeout(0)
             return 0.0

        # Identify potential future stops on this bus's route
        current_stop_id = self.get_current_stop_id()
        try:
            current_route_index = self.route_stops.index(current_stop_id)
            valid_future_stops = self.route_stops[current_route_index + 1:]
        except ValueError:
            logger.error(f"Current stop '{current_stop_id}' not found in route for bus {self.bus_id}. Skipping boarding.")
            yield self.env.timeout(0)
            return 0.0

        # --- Check 2: Are there any valid future stops? ---
        if not valid_future_stops:
             logger.debug(f"Time {board_start_time_str}:   Boarding skipped at {current_bus_stop.stop_id} (No subsequent stops on route).")
             yield self.env.timeout(0)
             return 0.0

        initial_waiting_count = len(current_bus_stop.waiting_passengers.items)
        logger.debug(f"Time {board_start_time_str}:   Attempting boarding at {current_bus_stop.stop_id}. Initial Waiting: {initial_waiting_count}. Valid destinations: {valid_future_stops}")

        passengers_to_put_back = []

        # Process passengers up to the number initially waiting
        while processed_passengers_count < initial_waiting_count:
            # Check if bus is full *before* getting next passenger
            if self.current_capacity_load >= BUS_CAPACITY:
                 # Check disabled capacity specifically only if bus has general capacity left
                 if not (self.current_capacity_load < BUS_CAPACITY and self.disabled_passenger_count >= MAX_DISABLED_PASSENGERS_PER_BUS):
                     logger.debug(f"Time {format_time(self.env.now)}:     Bus full (Load: {self.current_capacity_load}/{BUS_CAPACITY}, Disabled: {self.disabled_passenger_count}/{MAX_DISABLED_PASSENGERS_PER_BUS}). Stopping boarding attempt.")
                     break # Stop trying to get more passengers

            # Try to get the next passenger
            if not current_bus_stop.waiting_passengers.items: # Check if store is empty before getting
                 logger.debug(f"Time {format_time(self.env.now)}:     Waiting queue at {current_bus_stop.stop_id} is now empty.")
                 break

            try:
                 passenger = yield current_bus_stop.waiting_passengers.get()
                 processed_passengers_count += 1 # Increment only after successful get
            except simpy.Interrupt:
                 logger.warning(f"Boarding interrupted while getting passenger from {current_bus_stop.stop_id}")
                 break
            except (RuntimeError, simpy.core.StopSimulation) as e:
                logger.warning(f"Could not get passenger from {current_bus_stop.stop_id}, store might be empty or error: {e}")
                break

            # --- Check 3: Is the passenger's destination reachable by THIS bus from here? ---
            passenger_destination = passenger.destination_stop_id
            # passenger_intended_route = passenger.intended_route_id # Get intended route
            # Optional: Check if passenger_intended_route == self.bus_id (or derived route ID)?
            # Might be too strict if routes overlap partially.
            
            if passenger_destination not in valid_future_stops:
                # Destination is not on the remaining part of this bus's route
                logger.debug(f"Time {board_start_time_str}:     {passenger} cannot board Bus {self.bus_id}. Destination '{passenger_destination}' not on remaining route {valid_future_stops}. Re-queuing.")
                skipped_invalid_dest += 1
                passengers_to_put_back.append(passenger) # Put back for other potential buses/routes
                continue # Process next passenger

            # --- Check 4: Capacity constraints ---
            can_board_disabled = (passenger.is_disabled and
                                  self.disabled_passenger_count < MAX_DISABLED_PASSENGERS_PER_BUS and
                                  self.current_capacity_load + passenger.capacity_cost <= BUS_CAPACITY)
            can_board_regular = (not passenger.is_disabled and
                                 self.current_capacity_load + passenger.capacity_cost <= BUS_CAPACITY)

            if can_board_disabled or can_board_regular:
                # --- Board the passenger ---
                self.passengers.append(passenger)
                self.current_capacity_load += passenger.capacity_cost
                if passenger.is_disabled:
                    self.disabled_passenger_count += 1

                boarding_time_for_passenger = passenger.boarding_time
                yield self.env.timeout(boarding_time_for_passenger) # Simulate boarding time
                total_boarding_time += boarding_time_for_passenger
                boarded_count += 1

                board_event_time_str = format_time(self.env.now) # Time after boarding timeout
                logger.debug(f"Time {board_event_time_str}:     {passenger} boarded Bus {self.bus_id} at {current_bus_stop.stop_id} (Destination: {passenger.destination_stop_id}). Boarding time: {boarding_time_for_passenger:.2f}s. New Load: {self.current_capacity_load}/{BUS_CAPACITY} ({self.disabled_passenger_count} disabled)")
                self.log.append((board_event_time_str, "board", passenger.id, current_bus_stop.stop_id, passenger.destination_stop_id))
                
                # Record event to EventHandler
                if self.event_handler:
                    self.event_handler.record_event('passenger_boarded', {
                        'bus_id': self.bus_id,
                        'passenger_id': passenger.id,
                        'stop_id': current_bus_stop.stop_id,
                        'destination': passenger.destination_stop_id
                    })
            else:
                # --- Cannot board due to capacity ---
                denied_capacity += 1
                reason = "bus full"
                if passenger.is_disabled and self.disabled_passenger_count >= MAX_DISABLED_PASSENGERS_PER_BUS and self.current_capacity_load + passenger.capacity_cost <= BUS_CAPACITY:
                    reason = "disabled capacity full"
                elif self.current_capacity_load + passenger.capacity_cost > BUS_CAPACITY:
                     reason = f"capacity ({self.current_capacity_load + passenger.capacity_cost} > {BUS_CAPACITY})"

                # Decide if passenger requeues or leaves (based on REQUEUE_PROPORTION)
                if random.random() < REQUEUE_PROPORTION:
                     logger.debug(f"Time {board_start_time_str}:     {passenger} DENIED boarding Bus {self.bus_id} at {current_bus_stop.stop_id} ({reason}). Re-queuing.")
                     self.log.append((board_start_time_str, "denied_boarding_requeue", passenger.id, current_bus_stop.stop_id, reason))
                     passengers_to_put_back.append(passenger) # Put back into the queue
                else:
                     logger.debug(f"Time {board_start_time_str}:     {passenger} DENIED boarding Bus {self.bus_id} at {current_bus_stop.stop_id} ({reason}). Leaving queue.")
                     self.log.append((board_start_time_str, "denied_boarding_leave", passenger.id, current_bus_stop.stop_id, reason))
                     # Passenger is already out of the queue due to the initial 'get', so just don't put back.

        # --- After processing the initial queue snapshot, put back those who need to wait ---
        if passengers_to_put_back:
            put_back_start_time = self.env.now
            logger.debug(f"Time {format_time(put_back_start_time)}:     Putting {len(passengers_to_put_back)} passengers back into the queue at {current_bus_stop.stop_id}...")
            for p_back in passengers_to_put_back:
                # This might change the order, but ensures they wait
                yield current_bus_stop.waiting_passengers.put(p_back)
            logger.debug(f"Time {format_time(self.env.now)}:     Finished putting passengers back (took {self.env.now - put_back_start_time:.2f}s).")

        # Include skipped_invalid_dest in the final print
        logger.debug(f"Time {format_time(self.env.now)}:   Boarding attempt finished at {current_bus_stop.stop_id}. Processed: {processed_passengers_count}/{initial_waiting_count}. Boarded: {boarded_count}, Skipped Invalid Dest: {skipped_invalid_dest}, Denied Capacity: {denied_capacity}. Total boarding time: {total_boarding_time:.2f}s")
        return total_boarding_time # Return the total time spent boarding this cycle

# Interface Definition:
# - Initialization: Create Bus(env, bus_id, route, bus_stops)
# - Access Data: bus.log contains event logs.
# - Access State: bus.passengers, bus.current_capacity_load, bus.disabled_passenger_count
# - The `run_route` process starts automatically.
# - Interaction with BusStop happens internally via methods.
