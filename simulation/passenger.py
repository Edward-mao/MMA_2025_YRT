import itertools
import os
import sys

# Ensure script can find simulation package
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

try:
    from simulation.config import (
        REGULAR_BOARDING_TIME, DISABLED_BOARDING_TIME,
        REGULAR_ALIGHTING_TIME, DISABLED_ALIGHTING_TIME,
        REGULAR_CAPACITY_COST, DISABLED_CAPACITY_COST
    )
    # Import the time formatting utility
    from simulation.simulation_utils import format_time
except ImportError:
    from config import (
        REGULAR_BOARDING_TIME, DISABLED_BOARDING_TIME,
        REGULAR_ALIGHTING_TIME, DISABLED_ALIGHTING_TIME,
        REGULAR_CAPACITY_COST, DISABLED_CAPACITY_COST
    )
    # Import the time formatting utility
    from simulation_utils import format_time

class Passenger:
    """Represents a passenger in the simulation."""
    id_iter = itertools.count() # Unique ID generator for each passenger

    def __init__(self, env, arrival_time_at_stop, origin_stop_id, destination_stop_id, intended_route_id: str, is_disabled=False):
        """
        Initializes a Passenger instance.

        Args:
            env: The SimPy environment.
            arrival_time_at_stop: The simulation time when the passenger arrives at the origin stop.
            origin_stop_id: The ID of the stop where the passenger starts waiting.
            destination_stop_id: The ID of the passenger's destination stop.
            intended_route_id: The ID of the route the passenger intends to take.
            is_disabled: Boolean indicating if the passenger is disabled.
        """
        self.env = env
        self.id = next(Passenger.id_iter)
        self.arrival_time_at_stop = arrival_time_at_stop
        self.origin_stop_id = origin_stop_id
        self.destination_stop_id = destination_stop_id
        self.intended_route_id = intended_route_id
        self.is_disabled = is_disabled
        self.boarding_time_on_bus = None # Time when the passenger successfully boards

        # Assign properties based on passenger type
        if self.is_disabled:
            self.boarding_time = DISABLED_BOARDING_TIME
            self.alighting_time = DISABLED_ALIGHTING_TIME
            self.capacity_cost = DISABLED_CAPACITY_COST
            self.type = "Disabled"
        else:
            self.boarding_time = REGULAR_BOARDING_TIME
            self.alighting_time = REGULAR_ALIGHTING_TIME
            self.capacity_cost = REGULAR_CAPACITY_COST
            self.type = "Regular"

    def __repr__(self):
        # Format the arrival time using the utility function
        formatted_arrival_time = format_time(self.arrival_time_at_stop)
        return (f"Passenger(ID={self.id}, Type={self.type}, Route={self.intended_route_id}, Origin={self.origin_stop_id}, "
                f"Dest={self.destination_stop_id}, ArrivedAt='{formatted_arrival_time}')")

# Interface Definition for other modules:
# - Other modules can create Passenger objects using the constructor.
# - Access passenger attributes like passenger.id, passenger.destination_stop_id, etc.
# - No specific methods exposed for external interaction beyond attribute access for now.
