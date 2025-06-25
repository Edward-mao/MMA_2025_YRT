"""
SimPy data collection hook.
Captures key events during the simulation and outputs them to a queue.
"""
import queue
import json
import logging
from datetime import datetime, date
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
import threading
import uuid
import os
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class BusEvent:
    """Data structure for bus events."""
    # Basic fields
    sign_id: str  # Unique ID
    opd_date: str  # Operating date YYYY-MM-DD
    weekday: int  # Day of the week (1-7)
    block: str  # Time period (e.g., "morning", "peak")
    line_abbr: str  # Line ID
    direction: str  # Direction
    trip_id_int: str  # Trip ID
    
    # Time-related
    sched_arr_time: float  # Scheduled arrival time (seconds)
    act_arr_time: float  # Actual arrival time (seconds)
    sched_dep_time: float  # Scheduled departure time (seconds)
    act_dep_time: float  # Actual departure time (seconds)
    dwell_time: float  # Dwell time (seconds)
    
    # Trip-related
    sched_trip_time: float  # Scheduled trip time (seconds)
    act_trip_time: float  # Actual trip time (seconds)
    diff_trip_time: float  # Trip time difference (seconds)
    diff_dep_time: float  # Departure time difference (seconds)
    
    # Location-related
    stop_id: str  # Current stop ID
    stop_sequence: int  # Stop sequence number
    sched_distance: float  # Scheduled distance (meters)
    act_speed: float  # Actual speed (km/h)
    distance_to_next: float  # Distance to next stop (meters)
    distance_to_trip: float  # Distance to end of trip (meters)
    
    # Passenger-related
    boarding: int  # Number of passengers boarding
    alighting: int  # Number of passengers alighting
    load: int  # Number of passengers on board
    wheelchair_count: int  # Number of wheelchair passengers
    
    # Other
    is_additional: bool  # Whether it is an extra vehicle
    event_type: str  # Event type: 'arrival' or 'departure'
    timestamp: float  # Event timestamp (SimPy time)
    

class SimPyDataHook:
    """
    SimPy data collection hook.
    Captures key events during the simulation and outputs them to a queue.
    """
    
    def __init__(self, 
                 output_queue: Optional[queue.Queue] = None,
                 output_dir: str = "./data_output",
                 batch_size: int = 1000,
                 log_level: str = "INFO",
                 simulation_date: Optional[str] = None):
        """
        Initializes the data collection hook.
        
        Args:
            output_queue: The output queue; a new queue is created if None.
            output_dir: The output file directory.
            batch_size: The batch write size.
            log_level: The logging level.
            simulation_date: The date selected by the simulator (format: YYYY-MM-DD).
        """
        self.output_queue = output_queue or queue.Queue()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.batch_size = batch_size
        
        # Set the logging level
        logger.setLevel(getattr(logging, log_level.upper()))
        
        # Save the date selected by the simulator
        self.simulation_date = simulation_date
        if self.simulation_date:
            self.simulation_weekday = datetime.strptime(self.simulation_date, "%Y-%m-%d").isoweekday()
        else:
            # Use the current date as a fallback if not specified
            self.simulation_date = date.today().strftime("%Y-%m-%d")
            self.simulation_weekday = date.today().isoweekday()
        
        logger.info(f"SimPyDataHook initialized with simulation date: {self.simulation_date}")
        
        # Internal cache
        self._event_buffer = []
        self._trip_start_times: Dict[str, float] = {}  # Records the start time of each trip
        self._previous_stop_data: Dict[str, Dict] = {}  # Records data from the previous stop
        self._stop_distances: Dict[str, List[float]] = {}  # Distances between stops
        
        # Background writer thread
        self._writer_thread = None
        self._stop_writer = threading.Event()
        
    def start(self):
        """Starts the background writer thread."""
        if self._writer_thread is None:
            self._writer_thread = threading.Thread(target=self._writer_worker)
            self._writer_thread.daemon = True
            self._writer_thread.start()
            logger.info("SimPyDataHook background writer thread started")
            
    def stop(self):
        """Stops the background writer thread."""
        self._stop_writer.set()
        if self._writer_thread:
            self._writer_thread.join()
            logger.info("SimPyDataHook background writer thread stopped")
            
    def _writer_worker(self):
        """Background writer worker thread."""
        while not self._stop_writer.is_set():
            try:
                # Get an event from the queue (with a 1-second timeout)
                event = self.output_queue.get(timeout=1)
                self._event_buffer.append(event)
                
                # Write when the batch size is reached
                if len(self._event_buffer) >= self.batch_size:
                    self._flush_buffer()
                    
            except queue.Empty:
                # If the queue is empty, write any cached data
                if self._event_buffer:
                    self._flush_buffer()
                    
        # Write any remaining data before exiting
        if self._event_buffer:
            self._flush_buffer()
            
    def _flush_buffer(self):
        """Writes cached data to a file."""
        if not self._event_buffer:
            return
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Use .json extension to ensure Spark streaming can recognize it correctly
        filename = self.output_dir / f"bus_events_{timestamp}.json"
        logger.debug(f"Flush buffer to file: {filename}")
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                for event in self._event_buffer:
                    f.write(json.dumps(event, ensure_ascii=False) + '\n')
                    
            logger.info(f"Wrote {len(self._event_buffer)} events to {filename}")
            self._event_buffer.clear()
            
        except Exception as e:
            logger.error(f"Failed to write to file: {e}")
            
    def on_bus_arrival(self, 
                      env_time: float,
                      bus_id: str,
                      route_id: str,
                      stop_id: str,
                      stop_sequence: int,
                      scheduled_time: float,
                      passenger_load: int,
                      wheelchair_count: int = 0,
                      is_replacement: bool = False,
                      **kwargs):
        """
        Handles vehicle arrival events.
        
        Args:
            env_time: The SimPy environment time.
            bus_id: The vehicle ID.
            route_id: The route ID.
            stop_id: The stop ID.
            stop_sequence: The stop sequence number.
            scheduled_time: The scheduled arrival time.
            passenger_load: The current passenger load.
            wheelchair_count: The number of wheelchair passengers.
            is_replacement: Whether it is a replacement vehicle.
            **kwargs: Other parameters.
        """
        # Generate a unique ID
        sign_id = f"{bus_id}_{stop_id}_{int(env_time*1000)}_{uuid.uuid4().hex[:6]}"
        
        # Use the date selected by the simulator
        opd_date = self.simulation_date
        weekday = self.simulation_weekday
        
        # Determine the time period
        hour = int(env_time / 3600) % 24
        if 6 <= hour < 9:
            block = "morning_peak"
        elif 9 <= hour < 16:
            block = "midday"
        elif 16 <= hour < 19:
            block = "evening_peak"
        else:
            block = "off_peak"
            
        # Calculate trip time (if not the first stop)
        trip_key = f"{bus_id}_{route_id}"
        act_trip_time = 0
        sched_trip_time = 0
        diff_trip_time = 0
        
        if stop_sequence > 1 and trip_key in self._previous_stop_data:
            prev_data = self._previous_stop_data[trip_key]
            act_trip_time = env_time - prev_data['dep_time']
            sched_trip_time = scheduled_time - prev_data['sched_time']
            diff_trip_time = act_trip_time - sched_trip_time
            
        # Build the event data
        event = BusEvent(
            sign_id=sign_id,
            opd_date=opd_date,
            weekday=weekday,
            block=block,
            line_abbr=route_id.split('_')[0] if '_' in route_id else route_id,
            direction=kwargs.get('direction', 'unknown'),
            trip_id_int=f"{bus_id}_{route_id}",
            sched_arr_time=scheduled_time,
            act_arr_time=env_time,
            sched_dep_time=scheduled_time,  # Will be updated on departure
            act_dep_time=0,  # Will be updated on departure
            dwell_time=0,  # Will be updated on departure
            sched_trip_time=sched_trip_time,
            act_trip_time=act_trip_time,
            diff_trip_time=diff_trip_time,
            diff_dep_time=0,  # Will be updated on departure
            stop_id=stop_id,
            stop_sequence=stop_sequence,
            sched_distance=kwargs.get('distance', 0),
            act_speed=kwargs.get('speed', 0),
            distance_to_next=kwargs.get('distance_to_next', 0),
            distance_to_trip=kwargs.get('distance_to_trip', 0),
            boarding=0,  # Will be updated after processing passengers
            alighting=0,  # Will be updated after processing passengers
            load=passenger_load,
            wheelchair_count=wheelchair_count,
            is_additional=is_replacement,
            event_type='arrival',
            timestamp=env_time
        )
        
        # Cache the arrival data to be updated on departure
        self._cache_arrival_data(sign_id, event)
        
        logger.debug(f"Captured arrival event: Bus {bus_id} at Stop {stop_id}")
        
    def on_bus_departure(self,
                        env_time: float,
                        bus_id: str,
                        route_id: str,
                        stop_id: str,
                        boarded: int,
                        alighted: int,
                        dwell_time: float,
                        passenger_load: int,
                        **kwargs):
        """
        Handles vehicle departure events.
        
        Args:
            env_time: The SimPy environment time.
            bus_id: The vehicle ID.
            route_id: The route ID.
            stop_id: The stop ID.
            boarded: The number of passengers who boarded.
            alighted: The number of passengers who alighted.
            dwell_time: The dwell time.
            passenger_load: The passenger load on departure.
            **kwargs: Other parameters.
        """
        # Find the corresponding arrival event
        arrival_event = self._find_cached_arrival(bus_id, stop_id)
        
        if arrival_event:
            # Update departure-related fields
            arrival_event.act_dep_time = env_time
            arrival_event.diff_dep_time = env_time - arrival_event.sched_dep_time
            arrival_event.dwell_time = dwell_time
            arrival_event.boarding = boarded
            arrival_event.alighting = alighted
            arrival_event.load = passenger_load
            
            # Output to the queue
            self.output_queue.put(asdict(arrival_event))
            
            # Update the data for the previous stop
            trip_key = f"{bus_id}_{route_id}"
            self._previous_stop_data[trip_key] = {
                'dep_time': env_time,
                'sched_time': arrival_event.sched_dep_time,
                'stop_id': stop_id
            }
            
            logger.debug(f"Completed departure event: Bus {bus_id} from Stop {stop_id}")
            
        else:
            logger.warning(f"Corresponding arrival event not found: Bus {bus_id} at Stop {stop_id}")
            
    def _cache_arrival_data(self, sign_id: str, event: BusEvent):
        """Caches arrival event data."""
        # Use bus_id (without the part after the last '_') as part of the cache key to ensure consistency with the find logic
        # The original trip_id_int is like "{bus_id}_{route_id}", keep only the bus_id part
        bus_id_only = event.trip_id_int.rsplit('_', 1)[0]
        cache_key = f"{bus_id_only}_{event.stop_id}"

        # Store with a unified key name
        setattr(self, f"_arrival_{cache_key}", (sign_id, event))

        # Also keep the old format key (including route_id) for compatibility with potential other callers
        legacy_key = f"{event.trip_id_int}_{event.stop_id}"
        setattr(self, f"_arrival_{legacy_key}", (sign_id, event))
        
    def _find_cached_arrival(self, bus_id: str, stop_id: str) -> Optional[BusEvent]:
        """Finds a cached arrival event."""
        # Try different trip_id formats
        for route_suffix in ['', '_1', '_2']:  # Handle possible route suffixes
            cache_key = f"{bus_id}{route_suffix}_{stop_id}"
            attr_name = f"_arrival_{cache_key}"
            
            if hasattr(self, attr_name):
                sign_id, event = getattr(self, attr_name)
                delattr(self, attr_name)  # Clear the cache
                return event
                
        return None
        
    def set_stop_distances(self, route_id: str, distances: List[float]):
        """Sets the distances between stops for a route."""
        self._stop_distances[route_id] = distances
        logger.info(f"Set {len(distances)} stop-to-stop distances for route {route_id}")
        
    def get_statistics(self) -> Dict[str, Any]:
        """Gets statistics."""
        return {
            'queue_size': self.output_queue.qsize(),
            'buffer_size': len(self._event_buffer),
            'cached_trips': len(self._previous_stop_data),
            'output_dir': str(self.output_dir)
        } 