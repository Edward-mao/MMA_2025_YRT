"""
调度器接口定义
提供统一的调度器抽象接口，使得调度逻辑与系统其他部分解耦
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Callable
import simpy
from .bus_stop import BusStop
from .bus import Bus
from .event_handler import EventHandler
import logging
from collections import deque
import numpy as np


class SchedulerInterface(ABC):
    """Abstract interface for schedulers."""
    
    def __init__(self, env: simpy.Environment):
        """
        Initializes the scheduler.
        
        Args:
            env: The SimPy simulation environment.
        """
        self.env = env
        
    @abstractmethod
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
        Schedules buses for a specified route.
        
        Args:
            route_id: The route ID.
            route_stops: A list of stops on the route.
            bus_stops: A dictionary of BusStop objects.
            active_buses_list: A list of active buses.
            event_handler: The event handler.
            stop_mapping: Stop ID mapping, supports both old and new formats:
                Old format: {'simpy_id': 'sumo_id', ...}
                New format: {
                    'simpy_to_sumo': {
                        'northbound': {'simpy_id': 'sumo_id', ...},
                        'southbound': {'simpy_id': 'sumo_id', ...}
                    },
                    'sumo_routes': {...}
                }
            **kwargs: Other scheduler-specific parameters.
        """
        pass
        
    @abstractmethod
    def get_next_departure_time(self, route_id: str) -> Optional[float]:
        """
        Gets the next departure time for a specified route.
        
        Args:
            route_id: The route ID.
            
        Returns:
            The next departure time (in simulation time), or None if there is none.
        """
        pass
        
    @abstractmethod
    def update_schedule(self, route_id: str, schedule_data: Any) -> bool:
        """
        Updates the schedule for a specified route.
        
        Args:
            route_id: The route ID.
            schedule_data: New scheduling data.
            
        Returns:
            True if the update was successful, False otherwise.
        """
        pass
        
    def set_bus_creator(self, bus_creator: Callable) -> None:
        """
        Sets the bus creation function.
        
        Args:
            bus_creator: A function used to create buses.
        """
        self.bus_creator = bus_creator


class TimetableScheduler(SchedulerInterface):
    """Scheduler implementation based on a timetable."""
    
    def __init__(self, env: simpy.Environment, timetable_manager):
        """
        Initializes the timetable scheduler.
        
        Args:
            env: The SimPy simulation environment.
            timetable_manager: An instance of the timetable manager.
        """
        super().__init__(env)
        self.timetable_manager = timetable_manager
        self.route_schedules = {}  # Stores the scheduling process for each route
        
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
        """Schedules buses based on a timetable."""
        # Start the scheduling process for this route
        schedule_process = self.env.process(
            self._schedule_buses_for_route(
                route_id, route_stops, bus_stops, 
                active_buses_list, event_handler, stop_mapping, **kwargs
            )
        )
        self.route_schedules[route_id] = schedule_process
        
    def _schedule_buses_for_route(
        self, 
        route_id: str,
        route_stops: List[str],
        bus_stops: Dict[str, BusStop],
        active_buses_list: List[Bus],
        event_handler: EventHandler,
        stop_mapping: Dict[str, str],
        **kwargs
    ):
        """Coroutine to schedule buses for a specific route."""
        scenario_name = kwargs.get('scenario_name', '601')
        
        # Get the schedule for this route
        schedule = self.timetable_manager.get_schedule_for_route(route_id)
        if not schedule:
            yield self.env.timeout(0)  # No-op
            return
            
        # Process each trip by departure time
        for trip_data in schedule:
            departure_time = trip_data['departure_time']
            
            # Wait until the departure time
            wait_time = departure_time - self.env.now
            if wait_time > 0:
                yield self.env.timeout(wait_time)
            elif wait_time < -300:  # Skip if more than 5 minutes late
                continue
                
            # Call the bus creation function
            if hasattr(self, 'bus_creator'):
                self.bus_creator(
                    env=self.env,
                    bus_id=f"bus_{route_id}_{trip_data['trip_id']}_{int(self.env.now)}",
                    route_id=route_id,
                    route_stops=route_stops,
                    bus_stops=bus_stops,
                    active_buses_list=active_buses_list,
                    event_handler=event_handler,
                    trip_data=trip_data,
                    stop_mapping=stop_mapping,
                    scenario_name=scenario_name
                )
                
    def get_next_departure_time(self, route_id: str) -> Optional[float]:
        """Gets the next departure time."""
        return self.timetable_manager.get_next_departure_time(route_id, self.env.now)
        
    def update_schedule(self, route_id: str, schedule_data: Any) -> bool:
        """Updates the timetable."""
        return self.timetable_manager.update_schedule(route_id, schedule_data)
    
    def on_bus_arrival(self, bus: Bus, stop_id: str) -> float:
        """
        Handles bus arrival events, implementing timetable-based deviation correction.
        
        Args:
            bus: The arriving bus.
            stop_id: The stop ID.
            
        Returns:
            The recommended hold time (in seconds).
        """
        hold_time = 0
        
        # Check the scheduled arrival time for the current stop
        if hasattr(bus, 'stop_schedules') and stop_id in bus.stop_schedules:
            scheduled_time = bus.stop_schedules[stop_id]
            current_time = self.env.now
            deviation = current_time - scheduled_time
            
            # If arriving more than 30 seconds early, implement a hold
            if deviation < -30:
                # The timetable scheduler uses a relatively conservative correction strategy
                # Wait for at most 60% of the early arrival time, but no more than 120 seconds
                hold_time = min(abs(deviation) * 0.6, 120)
                
                logger.info(
                    f"Timetable schedule: Bus {bus.bus_id} at stop {stop_id} "
                    f"arrived {abs(deviation):.1f} seconds early, holding for {hold_time:.1f} seconds"
                )
        
        return hold_time


class IntervalScheduler(SchedulerInterface):
    """Scheduler implementation based on a fixed interval."""
    
    def __init__(self, env: simpy.Environment, default_interval: float = 1800):
        """
        Initializes the interval scheduler.
        
        Args:
            env: The SimPy simulation environment.
            default_interval: The default dispatch interval (in seconds).
        """
        super().__init__(env)
        self.default_interval = default_interval
        self.route_intervals = {}  # Dispatch interval for each route
        self.route_schedules = {}  # Stores the scheduling process for each route
        
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
        """Schedules buses at a fixed interval."""
        interval = kwargs.get('interval', self.default_interval)
        self.route_intervals[route_id] = interval
        
        # Start the scheduling process for this route
        schedule_process = self.env.process(
            self._schedule_buses_with_interval(
                route_id, route_stops, bus_stops, 
                active_buses_list, event_handler, stop_mapping, interval, **kwargs
            )
        )
        self.route_schedules[route_id] = schedule_process
        
    def _schedule_buses_with_interval(
        self, 
        route_id: str,
        route_stops: List[str],
        bus_stops: Dict[str, BusStop],
        active_buses_list: List[Bus],
        event_handler: EventHandler,
        stop_mapping: Dict[str, str],
        interval: float,
        **kwargs
    ):
        """Coroutine to schedule buses at a fixed interval."""
        scenario_name = kwargs.get('scenario_name', '601')
        bus_counter = 1
        
        while True:
            # Create a bus
            if hasattr(self, 'bus_creator'):
                trip_data = {
                    'trip_id': f'interval_{bus_counter}',
                    'departure_time': self.env.now,
                    'stops': [{'stop_id': stop, 'scheduled_time': self.env.now} for stop in route_stops]
                }
                
                self.bus_creator(
                    env=self.env,
                    bus_id=f"bus_{route_id}_interval_{bus_counter}_{int(self.env.now)}",
                    route_id=route_id,
                    route_stops=route_stops,
                    bus_stops=bus_stops,
                    active_buses_list=active_buses_list,
                    event_handler=event_handler,
                    trip_data=trip_data,
                    stop_mapping=stop_mapping,
                    scenario_name=scenario_name
                )
                
            bus_counter += 1
            yield self.env.timeout(interval)
            
    def get_next_departure_time(self, route_id: str) -> Optional[float]:
        """Gets the next departure time."""
        interval = self.route_intervals.get(route_id, self.default_interval)
        return self.env.now + interval
        
    def update_schedule(self, route_id: str, schedule_data: Any) -> bool:
        """Updates the dispatch interval."""
        if isinstance(schedule_data, (int, float)) and schedule_data > 0:
            self.route_intervals[route_id] = float(schedule_data)
            return True
        return False


class AdaptiveScheduler(SchedulerInterface):
    """
    Adaptive scheduler that dynamically adjusts the dispatch plan based on real-time conditions.
    """
    
    def __init__(self, env: simpy.Environment, timetable_manager, config: Dict[str, Any]):
        """
        Initializes the adaptive scheduler.
        
        Args:
            env: The SimPy simulation environment.
            timetable_manager: The timetable manager (used for the base plan).
            config: Scheduler configuration parameters.
        """
        super().__init__(env)
        self.timetable_manager = timetable_manager
        self.logger = logging.getLogger(__name__)
        
        # Configuration parameters
        # No longer uses a fixed headway_target; it is dynamically calculated from the timetable
        self.headway_tolerance = config.get('headway_tolerance', 0.15)  # Headway tolerance
        self.max_hold = config.get('max_hold', 120)  # Maximum hold time per stop (seconds)
        self.otp_window = config.get('otp_window', 180)  # OTP determination window (±3 minutes)
        self.demand_threshold = config.get('demand_threshold', 25)  # Passenger queue threshold at stops
        self.kpi_refresh_interval = config.get('kpi_refresh_interval', 300)  # KPI calculation period
        
        # Operational state
        self.route_schedules = {}  # Scheduling process for each route
        self.active_trips = {}  # Information on active trips
        self.departure_history = {}  # History of departures
        self.station_demand = {}  # Demand status at stations
        self.headway_history = {}  # History of headways
        self.planned_headways = {}  # Planned headways (calculated from the timetable)
        
        # Data collector reference (will be set after initialization)
        self.data_collector = None
        
    def set_data_collector(self, data_collector):
        """Sets the data collector."""
        self.data_collector = data_collector
        
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
        """Starts adaptive scheduling for the specified route."""
        # Initialize route-related data structures
        self.departure_history[route_id] = deque(maxlen=20)  # Keep the last 20 departure records
        self.headway_history[route_id] = deque(maxlen=10)  # Keep the last 10 headways
        self.active_trips[route_id] = []
        
        # Start the scheduling process
        schedule_process = self.env.process(
            self._adaptive_schedule_route(
                route_id, route_stops, bus_stops, 
                active_buses_list, event_handler, stop_mapping, **kwargs
            )
        )
        self.route_schedules[route_id] = schedule_process
        
    def _adaptive_schedule_route(
        self, 
        route_id: str,
        route_stops: List[str],
        bus_stops: Dict[str, BusStop],
        active_buses_list: List[Bus],
        event_handler: EventHandler,
        stop_mapping: Dict[str, str],
        **kwargs
    ):
        """Main loop for adaptive scheduling."""
        scenario_name = kwargs.get('scenario_name', '601')
        
        # Get the base timetable
        base_schedule = self.timetable_manager.get_schedule_for_route(route_id)
        if not base_schedule:
            self.logger.warning(f"No base timetable for route {route_id}")
            yield self.env.timeout(0)
            return
        
        # Pre-calculate planned headways for all trips
        self.planned_headways[route_id] = {}
        for i in range(1, len(base_schedule)):
            prev_departure = base_schedule[i-1]['departure_time']
            curr_departure = base_schedule[i]['departure_time']
            planned_headway = curr_departure - prev_departure
            trip_id = base_schedule[i]['trip_id']
            self.planned_headways[route_id][trip_id] = planned_headway
            self.logger.info(f"Planned headway for trip {trip_id}: {planned_headway}s")
            
        trip_index = 0
        
        # Find the first non-expired trip
        while trip_index < len(base_schedule):
            trip_data = base_schedule[trip_index]
            base_departure_time = trip_data['departure_time']
            
            # If the trip is already past by more than twice the OTP window, skip it
            if base_departure_time < self.env.now - self.otp_window * 2:
                self.logger.info(f"Skipping expired trip {trip_data['trip_id']} "
                               f"(Scheduled time={base_departure_time}, Current time={self.env.now}, "
                               f"Difference={self.env.now - base_departure_time}s, "
                               f"OTP window={self.otp_window}s)")
                trip_index += 1
                continue
            else:
                self.logger.info(f"Found first valid trip {trip_data['trip_id']} "
                               f"(Scheduled time={base_departure_time}, Current time={self.env.now})")
                break
        
        # Process the remaining trips
        while trip_index < len(base_schedule):
            trip_data = base_schedule[trip_index].copy()  # Copy to avoid modifying the original data
            base_departure_time = trip_data['departure_time']
            
            # Get the planned headway for the current trip
            current_trip_id = trip_data['trip_id']
            planned_headway = self.planned_headways[route_id].get(current_trip_id, None)
            
            # Calculate the adjusted departure time
            adjusted_departure_time = self._calculate_adjusted_departure_time(
                route_id, base_departure_time, route_stops, bus_stops, 
                trip_index, base_schedule
            )
            
            # Wait until the adjusted departure time
            wait_time = adjusted_departure_time - self.env.now
            if wait_time > 0:
                yield self.env.timeout(wait_time)
            elif wait_time < -self.otp_window * 2:  # Skip if severely delayed
                self.logger.info(f"Skipping severely delayed trip {trip_data['trip_id']}")
                trip_index += 1
                continue
                
            # Record the actual departure time
            actual_departure_time = self.env.now
            self.departure_history[route_id].append(actual_departure_time)
            
            # Update the headway history
            if len(self.departure_history[route_id]) >= 2:
                headway = self.departure_history[route_id][-1] - self.departure_history[route_id][-2]
                self.headway_history[route_id].append(headway)
            
            # Create the bus
            if hasattr(self, 'bus_creator'):
                # Update the departure time in trip_data
                trip_data['departure_time'] = actual_departure_time
                trip_data['scheduled_departure_time'] = base_departure_time  # Keep the original scheduled time
                
                bus = self.bus_creator(
                    env=self.env,
                    bus_id=f"bus_{route_id}_{trip_data['trip_id']}_{int(self.env.now)}",
                    route_id=route_id,
                    route_stops=route_stops,
                    bus_stops=bus_stops,
                    active_buses_list=active_buses_list,
                    event_handler=event_handler,
                    trip_data=trip_data,
                    stop_mapping=stop_mapping,
                    scenario_name=scenario_name
                )
                
                # Record the active trip
                if bus:  # Only record if the bus was created successfully
                    self.active_trips[route_id].append({
                        'bus': bus,
                        'trip_id': trip_data['trip_id'],
                        'departure_time': actual_departure_time
                    })
                    
                    self.logger.info(
                        f"Dispatch: Route={route_id}, Trip={trip_data['trip_id']}, "
                        f"Scheduled Time={base_departure_time}, Actual Time={actual_departure_time}, "
                        f"Deviation={actual_departure_time - base_departure_time:.1f}s"
                    )
                else:
                    self.logger.error(f"Failed to create bus: Route={route_id}, Trip={trip_data['trip_id']}")
            
            trip_index += 1
            
            # Check if an extra bus is needed
            if self._should_dispatch_extra_bus(route_id, route_stops, bus_stops):
                yield self.env.process(
                    self._dispatch_extra_bus(
                        route_id, route_stops, bus_stops, 
                        active_buses_list, event_handler, stop_mapping, 
                        scenario_name
                    )
                )
    
    def _calculate_adjusted_departure_time(
        self, 
        route_id: str, 
        base_time: float,
        route_stops: List[str],
        bus_stops: Dict[str, BusStop],
        trip_index: int,
        base_schedule: List[Dict[str, Any]]
    ) -> float:
        """
        Calculates the adjusted departure time.
        
        Returns:
            The adjusted departure time.
        """
        # If there is no historical data, use the base time
        if len(self.departure_history[route_id]) == 0:
            return base_time
            
        # Get the most recent departure time
        last_departure = self.departure_history[route_id][-1]
        time_since_last = base_time - last_departure
        
        # 1. Headway control
        headway_adjustment = self._calculate_headway_adjustment(route_id, time_since_last, trip_index, base_schedule)
        
        # 2. Demand-responsive adjustment
        demand_adjustment = self._calculate_demand_adjustment(route_id, route_stops, bus_stops)
        
        # 3. Combined adjustment (limited to a reasonable range)
        total_adjustment = headway_adjustment + demand_adjustment
        total_adjustment = max(-self.otp_window, min(self.otp_window, total_adjustment))
        
        # Calculate the final departure time
        adjusted_time = base_time + total_adjustment
        
        # Ensure it is not earlier than the current time
        return max(self.env.now, adjusted_time)
    
    def _calculate_headway_adjustment(self, route_id: str, time_since_last: float, 
                                     trip_index: int, base_schedule: List[Dict[str, Any]]) -> float:
        """
        Calculates the adjustment amount based on headway.
        """
        # Get the planned headway for the current trip
        if trip_index > 0:
            # Calculate the planned interval with the previous trip
            prev_departure = base_schedule[trip_index-1]['departure_time']
            curr_departure = base_schedule[trip_index]['departure_time']
            target_headway = curr_departure - prev_departure
        else:
            # First trip, no preceding bus, no adjustment
            return 0
        
        self.logger.debug(f"Planned headway: {target_headway}s, Actual interval: {time_since_last}s")
        
        # If the current interval is too large, depart earlier
        if time_since_last > target_headway * (1 + self.headway_tolerance):
            adjustment = -min(60, (time_since_last - target_headway) * 0.3)  # Advance by at most 1 minute
            self.logger.debug(f"Interval too large, recommending advance: {adjustment}s")
            return adjustment
            
        # If the current interval is too small, depart later
        if time_since_last < target_headway * (1 - self.headway_tolerance):
            adjustment = min(60, (target_headway - time_since_last) * 0.3)  # Delay by at most 1 minute
            self.logger.debug(f"Interval too small, recommending delay: {adjustment}s")
            return adjustment
            
        return 0
    
    def _calculate_demand_adjustment(
        self, 
        route_id: str,
        route_stops: List[str],
        bus_stops: Dict[str, BusStop]
    ) -> float:
        """
        Calculates the adjustment amount based on station demand.
        """
        # Get the queueing situation at stops
        total_waiting = 0
        critical_stops = 0
        
        for stop_id in route_stops[:5]:  # Only check the first 5 stops
            if stop_id in bus_stops:
                waiting_count = len(bus_stops[stop_id].waiting_passengers.items)
                total_waiting += waiting_count
                if waiting_count > self.demand_threshold:
                    critical_stops += 1
        
        # If multiple stops have long queues, depart earlier
        if critical_stops >= 2:
            return -30  # Advance by 30 seconds
        elif total_waiting > self.demand_threshold * 2:
            return -20  # Advance by 20 seconds
            
        return 0
    
    def _should_dispatch_extra_bus(
        self, 
        route_id: str,
        route_stops: List[str],
        bus_stops: Dict[str, BusStop]
    ) -> bool:
        """
        Determines if an extra bus is needed.
        """
        # Check the queueing situation at stops
        for stop_id in route_stops[:3]:  # Check the first 3 stops
            if stop_id in bus_stops:
                if len(bus_stops[stop_id].waiting_passengers.items) > self.demand_threshold * 2:
                    # Check if an extra bus was just dispatched
                    if len(self.departure_history[route_id]) >= 1:
                        time_since_last = self.env.now - self.departure_history[route_id][-1]
                        # Use the average of recent headways, or a default of 300 seconds
                        if len(self.headway_history[route_id]) > 0:
                            avg_headway = np.mean(list(self.headway_history[route_id]))
                        else:
                            avg_headway = 300  # Default 5 minutes
                        if time_since_last > avg_headway * 0.5:  # At least half an average headway apart
                            return True
        return False
    
    def _dispatch_extra_bus(
        self,
        route_id: str,
        route_stops: List[str],
        bus_stops: Dict[str, BusStop],
        active_buses_list: List[Bus],
        event_handler: EventHandler,
        stop_mapping: Dict[str, str],
        scenario_name: str
    ):
        """
        Dispatches an extra bus.
        """
        self.logger.info(f"Dispatching extra bus on route {route_id} in response to high demand")
        
        # Create trip_data for the extra bus
        trip_data = {
            'trip_id': f'extra_{int(self.env.now)}',
            'departure_time': self.env.now,
            'is_extra': True,
            'stops': [{'stop_id': stop, 'scheduled_time': self.env.now} for stop in route_stops]
        }
        
        # Record the departure
        self.departure_history[route_id].append(self.env.now)
        
        # Create the bus
        if hasattr(self, 'bus_creator'):
            bus = self.bus_creator(
                env=self.env,
                bus_id=f"bus_{route_id}_extra_{int(self.env.now)}",
                route_id=route_id,
                route_stops=route_stops,
                bus_stops=bus_stops,
                active_buses_list=active_buses_list,
                event_handler=event_handler,
                trip_data=trip_data,
                stop_mapping=stop_mapping,
                scenario_name=scenario_name
            )
            
            if bus:
                self.logger.info(f"Extra bus created successfully: {bus.bus_id}")
            else:
                self.logger.error(f"Failed to create extra bus")
        
        yield self.env.timeout(0)
    
    def get_next_departure_time(self, route_id: str) -> Optional[float]:
        """Gets the next estimated departure time."""
        # Get the base time from the timetable
        base_time = self.timetable_manager.get_next_departure_time(route_id, self.env.now)
        if base_time is None:
            return None
            
        # Simple estimation of adjustment
        if len(self.departure_history[route_id]) > 0:
            last_departure = self.departure_history[route_id][-1]
            time_since_last = base_time - last_departure
            # Use the average of recent headways
            if len(self.headway_history[route_id]) > 0:
                avg_headway = np.mean(list(self.headway_history[route_id]))
                if time_since_last < avg_headway * 0.8:
                    return base_time + 30  # May be delayed
        
        return base_time
        
    def update_schedule(self, route_id: str, schedule_data: Any) -> bool:
        """Updates scheduling parameters."""
        if isinstance(schedule_data, dict):
            # Update control parameters
            if 'headway_tolerance' in schedule_data:
                self.headway_tolerance = schedule_data['headway_tolerance']
            if 'max_hold' in schedule_data:
                self.max_hold = schedule_data['max_hold']
            if 'otp_window' in schedule_data:
                self.otp_window = schedule_data['otp_window']
            if 'demand_threshold' in schedule_data:
                self.demand_threshold = schedule_data['demand_threshold']
            return True
        return False
    
    def on_bus_arrival(self, bus: Bus, stop_id: str):
        """
        Handles bus arrival events (for Holding strategy).
        Enhanced version: dynamically adjusts hold time based on deviation at each stop.
        """
        hold_time = 0
        
        # Check the scheduled arrival time for the current stop
        if hasattr(bus, 'stop_schedules') and stop_id in bus.stop_schedules:
            scheduled_time = bus.stop_schedules[stop_id]
            current_time = self.env.now
            deviation = current_time - scheduled_time
            
            # Log deviation information
            self.logger.debug(
                f"Bus {bus.bus_id} at stop {stop_id}: "
                f"Scheduled time={scheduled_time}, Actual time={current_time}, "
                f"Deviation={deviation:.1f}s"
            )
            
            # Dynamic deviation correction strategy
            if deviation < -30:  # Arrived more than 30 seconds early
                # Determine hold time based on earliness and trip progress
                route_progress = bus.current_stop_index / len(bus.route_stops)
                
                if route_progress < 0.3:  # First 30% of the trip
                    # Hold for longer at the beginning to avoid accumulating larger deviations later
                    hold_time = min(abs(deviation) * 0.8, self.max_hold)
                elif route_progress < 0.7:  # Middle part of the trip
                    # Moderate hold time in the middle
                    hold_time = min(abs(deviation) * 0.5, self.max_hold * 0.7)
                else:  # Last part of the trip
                    # Shorter hold time at the end to avoid excessive total trip time
                    hold_time = min(abs(deviation) * 0.3, self.max_hold * 0.5)
                
                self.logger.info(
                    f"Bus {bus.bus_id} at stop {stop_id} performing corrective holding, "
                    f"arrived {abs(deviation):.1f} seconds early, holding for {hold_time:.1f} seconds, "
                    f"trip progress {route_progress:.1%}"
                )
                
                # Record holding event
                if hasattr(bus, 'event_handler') and bus.event_handler:
                    bus.event_handler.record_event('headway_adjust', {
                        'bus_id': bus.bus_id,
                        'stop_id': stop_id,
                        'hold_time': hold_time,
                        'deviation': deviation,
                        'route_progress': route_progress
                    })
            
            elif deviation > 180:  # More than 3 minutes late
                # No holding for severely late buses, log a warning
                self.logger.warning(
                    f"Bus {bus.bus_id} at stop {stop_id} is severely late by "
                    f"{deviation:.1f} seconds, skipping hold"
                )
        
        # Check headway (if information about the preceding bus is available)
        elif hasattr(bus, 'trip_data') and 'scheduled_departure_time' in bus.trip_data:
            # Use the original simple strategy based on the initial departure time as a fallback
            scheduled_time = bus.trip_data.get('scheduled_departure_time', 0)
            early_arrival = scheduled_time - self.env.now
            
            if early_arrival > 60:
                hold_time = min(early_arrival * 0.5, self.max_hold)
                self.logger.info(
                    f"Bus {bus.bus_id} at stop {stop_id} performing fallback holding strategy, "
                    f"holding for {hold_time:.1f} seconds"
                )
        
        return hold_time 