"""
Event handling module for the bus simulation system.
Handles system events like bus failures to avoid circular dependencies.
"""
import simpy
from typing import Dict, List, TYPE_CHECKING, Any, Optional
import logging
from collections import defaultdict, deque
import numpy as np
from datetime import datetime
import json
import os

from .simulation_utils import format_time

# Use TYPE_CHECKING to avoid circular imports during runtime
if TYPE_CHECKING:
    from bus import Bus
    from bus_stop import BusStop

logger = logging.getLogger(__name__)

class EventHandler:
    """Central event handler for the simulation system."""
    
    def __init__(self, env: simpy.Environment, enable_data_collection: bool = False, enable_kpi: bool = False):
        """Initialize the event handler.
        
        Args:
            env: The SimPy environment.
            enable_data_collection: Enable real-time data collection features
            enable_kpi: Enable KPI calculation and reporting
        """
        self.env = env
        self.event_log = []
        self.enable_data_collection = enable_data_collection
        self.enable_kpi = enable_kpi
        
        # Data collection attributes (only initialized if enabled)
        if self.enable_data_collection:
            # Real-time data storage
            self.bus_positions = {}  # Current position of buses
            self.bus_states = {}  # Bus state (running/dwelling)
            self.stop_queues = {}  # Number of people queuing at stops
            self.travel_times = defaultdict(lambda: deque(maxlen=20))  # History of travel times between stops
            self.dwell_times = defaultdict(lambda: deque(maxlen=20))  # History of dwell times at stops
            self.headway_deviations = defaultdict(list)  # Headway deviations
            self.schedule_adherence = defaultdict(list)  # Timetable adherence
            
            # Statistical data
            self.boarding_success_rate = defaultdict(lambda: {'success': 0, 'total': 0})
            self.passenger_wait_times = defaultdict(lambda: deque(maxlen=100))
            self.bus_load_factors = defaultdict(lambda: deque(maxlen=50))
            
            # Internal state
            self._last_departure = {}
            self._arrival_times = {}
            
            # Register event listeners
            self._register_data_collection_listeners()
        
        # KPI attributes (only initialized if enabled)
        if self.enable_kpi:
            # KPI configuration
            self.otp_window = 180  # OTP determination window (±3 minutes)
            self.kpi_refresh_interval = 300  # KPI calculation period (seconds)
            self.export_interval = 3600  # Export period (seconds)
            
            # KPI storage
            self.kpi_history = {
                'otp': [],  # On-time performance history
                'avg_wait_time': [],  # Average wait time history
                'headway_regularity': [],  # Headway regularity history
                'passenger_satisfaction': [],  # Passenger satisfaction history
                'system_efficiency': []  # System efficiency history
            }
            
            # Real-time KPIs
            self.current_kpis = {}
            
            # Start KPI calculation process
            if self.enable_data_collection:  # KPIs require data collection
                self.env.process(self._kpi_calculation_process())
                self.env.process(self._kpi_export_process())
        
    def record_event(self, *args) -> None:
        """Record an event.

        Supports two signatures for backward-compatibility:
        1) record_event(event_type: str, details: Dict)
        2) record_event(time: float, event_type: str, details: Dict)
        The first form uses current env.now automatically.
        """
        if len(args) == 2:
            event_type, details = args
            event_time = self.env.now
        elif len(args) == 3:
            event_time, event_type, details = args
        else:
            raise ValueError("record_event expects 2 or 3 positional arguments")

        self.event_log.append({
            'time': event_time,
            'event_type': event_type,
            'details': details
        })
        
        # Process event for data collection if enabled
        self._process_event_for_data_collection(event_type, details)
        
    def handle_bus_failure(self, 
                          failed_bus: 'Bus', 
                          all_bus_stops: Dict[str, 'BusStop'], 
                          active_buses_list: List['Bus']) -> None:
        """
        Handles the dispatch of a new bus when a failure occurs.
        
        Args:
            failed_bus: The bus object that failed.
            all_bus_stops: Dictionary mapping stop_id to BusStop objects.
            active_buses_list: A list containing currently active bus processes.
        """
        failure_time_str = format_time(self.env.now)
        
        # Log the event
        self.event_log.append({
            'time': self.env.now,
            'event_type': 'bus_failure',
            'bus_id': failed_bus.bus_id,
            'details': f"Bus {failed_bus.bus_id} failed"
        })
        
        logger.info(f"EVENT_HANDLER [{failure_time_str}]: Processing failure for {failed_bus.bus_id}")
        
        original_route = failed_bus.route
        if not original_route:
            error_msg = f"Failed bus {failed_bus.bus_id} has no route defined. Cannot dispatch replacement."
            logger.error(f"EVENT_HANDLER [{failure_time_str}]: {error_msg}")
            self.event_log.append({
                'time': self.env.now,
                'event_type': 'replacement_error',
                'bus_id': failed_bus.bus_id,
                'details': error_msg
            })
            return
            
        start_stop_id = original_route[0]
        replacement_bus_id = f"{failed_bus.bus_id}_Repl_{int(self.env.now)}"
        
        logger.info(f"EVENT_HANDLER [{failure_time_str}]: Creating replacement bus {replacement_bus_id} for route starting at {start_stop_id}.")
        
        # Import Bus here to avoid circular dependency at module level
        from .bus import Bus
        
        # Create and start the new Bus process
        new_bus = Bus(
            env=self.env,
            bus_id=replacement_bus_id,
            route=original_route, 
            bus_stops=all_bus_stops,
            active_buses_list=active_buses_list,
            event_handler=self  # Pass event handler reference
        )
        
        active_buses_list.append(new_bus)
        
        # Log replacement dispatch
        self.event_log.append({
            'time': self.env.now,
            'event_type': 'replacement_dispatched',
            'bus_id': replacement_bus_id,
            'original_bus_id': failed_bus.bus_id,
            'details': f"Replacement bus {replacement_bus_id} dispatched"
        })
        
        logger.info(f"EVENT_HANDLER [{failure_time_str}]: Replacement bus {replacement_bus_id} dispatched and added to active list.")
        
    def get_event_summary(self) -> Dict[str, int]:
        """Get a summary of events that occurred during simulation.
        
        Returns:
            Dictionary with event type counts.
        """
        summary = {}
        for event in self.event_log:
            event_type = event['event_type']
            summary[event_type] = summary.get(event_type, 0) + 1
        return summary
    
    # === Data Collection Methods ===
    
    def _register_data_collection_listeners(self):
        """Register event listeners for data collection."""
        # This method is called during initialization to set up event processing
        pass
    
    def _process_event_for_data_collection(self, event_type: str, details: Dict[str, Any]):
        """Process events for data collection if enabled."""
        if not self.enable_data_collection:
            return
            
        # Route events to appropriate handlers
        if event_type == 'bus_arrival':
            self._on_bus_arrival(details)
        elif event_type == 'bus_departure':
            self._on_bus_departure(details)
        elif event_type == 'bus_dispatch':
            self._on_bus_dispatch(details)
        elif event_type == 'passenger_arrival':
            self._on_passenger_arrival(details)
        elif event_type == 'passenger_boarding':
            self._on_passenger_boarding(details)
        elif event_type == 'passenger_alighting':
            self._on_passenger_alighting(details)
        elif event_type == 'passenger_denied_boarding':
            self._on_passenger_denied_boarding(details)
        elif event_type == 'headway_adjust':
            self._on_headway_adjust(details)
    
    def _on_bus_arrival(self, data: Dict[str, Any]):
        """Handles bus arrival events."""
        bus_id = data['bus_id']
        stop_id = data['stop_id']
        arrival_time = data.get('time', self.env.now)
        
        # Update bus position and state
        self.bus_positions[bus_id] = stop_id
        self.bus_states[bus_id] = 'at_stop'
        
        # Record travel time between stops
        if bus_id in self._last_departure:
            travel_time = arrival_time - self._last_departure[bus_id]['time']
            route_segment = f"{self._last_departure[bus_id]['stop']}->{stop_id}"
            self.travel_times[route_segment].append(travel_time)
        
        # Record arrival time
        if bus_id not in self._arrival_times:
            self._arrival_times[bus_id] = {}
        self._arrival_times[bus_id][stop_id] = arrival_time
        
        # Check timetable adherence
        if 'scheduled_time' in data:
            deviation = arrival_time - data['scheduled_time']
            self.schedule_adherence[bus_id].append({
                'stop': stop_id,
                'deviation': deviation,
                'time': arrival_time
            })
    
    def _on_bus_departure(self, data: Dict[str, Any]):
        """Handles bus departure events."""
        bus_id = data['bus_id']
        stop_id = data['stop_id']
        departure_time = data.get('time', self.env.now)
        
        # Update bus state
        self.bus_states[bus_id] = 'running'
        
        # Record dwell time
        if bus_id in self._arrival_times and stop_id in self._arrival_times[bus_id]:
            dwell_time = departure_time - self._arrival_times[bus_id][stop_id]
            self.dwell_times[stop_id].append(dwell_time)
        
        # Save departure information
        self._last_departure[bus_id] = {
            'stop': stop_id,
            'time': departure_time
        }
    
    def _on_bus_dispatch(self, data: Dict[str, Any]):
        """Handles bus dispatch events."""
        bus_id = data['bus_id']
        route_id = data.get('route_id', '')
        
        # Initialize bus data
        self.bus_positions[bus_id] = 'depot'
        self.bus_states[bus_id] = 'dispatched'
        
        # Record headway data
        if route_id:
            self.headway_deviations[route_id].append({
                'bus_id': bus_id,
                'time': self.env.now
            })
    
    def _on_passenger_arrival(self, data: Dict[str, Any]):
        """Handles passenger arrival events."""
        stop_id = data['stop_id']
        
        # Update stop queue length
        if stop_id not in self.stop_queues:
            self.stop_queues[stop_id] = 0
        self.stop_queues[stop_id] += 1
    
    def _on_passenger_boarding(self, data: Dict[str, Any]):
        """Handles passenger boarding events."""
        stop_id = data['stop_id']
        wait_time = data.get('wait_time', 0)
        
        # Update stop queue length
        if stop_id in self.stop_queues and self.stop_queues[stop_id] > 0:
            self.stop_queues[stop_id] -= 1
            
        # Record wait time
        self.passenger_wait_times[stop_id].append(wait_time)
        
        # Update boarding success rate
        self.boarding_success_rate[stop_id]['success'] += 1
        self.boarding_success_rate[stop_id]['total'] += 1
    
    def _on_passenger_denied_boarding(self, data: Dict[str, Any]):
        """Handles passenger denied boarding events."""
        stop_id = data['stop_id']
        
        # Update boarding success rate
        self.boarding_success_rate[stop_id]['total'] += 1
    
    def _on_passenger_alighting(self, data: Dict[str, Any]):
        """Handles passenger alighting events."""
        # Can be used to calculate bus load factor, etc.
        pass
    
    def _on_headway_adjust(self, data: Dict[str, Any]):
        """Handles headway adjustment events."""
        bus_id = data.get('bus_id')
        hold_time = data.get('hold_time', 0)
        current_headway = data.get('current_headway')
        target_headway = data.get('target_headway')
        stop_id = data.get('stop_id')
        
        # Record adjustment information
        logger.info(f"HEADWAY_ADJUST: Bus {bus_id} at stop {stop_id} - "
                   f"hold {hold_time}s to adjust headway from {current_headway}s to {target_headway}s")
    
    def get_stop_queue_length(self, stop_id: str) -> int:
        """Gets the current queue length at a stop."""
        if not self.enable_data_collection:
            return 0
        return self.stop_queues.get(stop_id, 0)
    
    def get_stop_performance(self, stop_id: str) -> Dict[str, Any]:
        """Gets performance metrics for a stop."""
        if not self.enable_data_collection:
            return {}
            
        result = {
            'current_queue': self.get_stop_queue_length(stop_id),
            'avg_wait_time': None,
            'boarding_success_rate': None,
            'avg_dwell_time': None
        }
        
        # Average wait time
        if stop_id in self.passenger_wait_times and len(self.passenger_wait_times[stop_id]) > 0:
            result['avg_wait_time'] = np.mean(list(self.passenger_wait_times[stop_id]))
            
        # Boarding success rate
        if stop_id in self.boarding_success_rate:
            stats = self.boarding_success_rate[stop_id]
            if stats['total'] > 0:
                result['boarding_success_rate'] = stats['success'] / stats['total']
                
        # Average dwell time
        if stop_id in self.dwell_times and len(self.dwell_times[stop_id]) > 0:
            result['avg_dwell_time'] = np.mean(list(self.dwell_times[stop_id]))
            
        return result
    
    # === KPI Methods ===
    
    def _kpi_calculation_process(self):
        """Process for periodically calculating KPIs."""
        while True:
            yield self.env.timeout(self.kpi_refresh_interval)
            
            # Calculate all KPIs
            self.current_kpis = self._calculate_all_kpis()
            
            # Save to history
            timestamp = self.env.now
            for kpi_name, kpi_value in self.current_kpis.items():
                if kpi_name in self.kpi_history:
                    self.kpi_history[kpi_name].append({
                        'time': timestamp,
                        'value': kpi_value
                    })
            
            # Log it
            logger.info(f"KPI Update @ {timestamp}: {self._format_kpi_summary()}")
    
    def _kpi_export_process(self):
        """Process for periodically exporting KPIs."""
        while True:
            yield self.env.timeout(self.export_interval)
            self.export_kpis()
    
    def _calculate_all_kpis(self) -> Dict[str, Any]:
        """Calculates all KPIs."""
        kpis = {}
        
        # 1. On-Time Performance
        kpis['otp'] = self._calculate_overall_otp()
        
        # 2. Average Wait Time
        kpis['avg_wait_time'] = self._calculate_average_wait_time()
        
        # 3. Headway Regularity
        kpis['headway_regularity'] = self._calculate_headway_regularity()
        
        # 4. Passenger Satisfaction (composite index)
        kpis['passenger_satisfaction'] = self._calculate_passenger_satisfaction()
        
        # 5. System Efficiency
        kpis['system_efficiency'] = self._calculate_system_efficiency()
        
        return kpis
    
    def _calculate_overall_otp(self) -> float:
        """Calculates overall on-time performance."""
        total_on_time = 0
        total_trips = 0
        
        for bus_id, adherence_list in self.schedule_adherence.items():
            for record in adherence_list:
                if abs(record['deviation']) <= self.otp_window:
                    total_on_time += 1
                total_trips += 1
                
        return total_on_time / total_trips if total_trips > 0 else 0.0
    
    def _calculate_average_wait_time(self) -> float:
        """Calculates the system-wide average wait time."""
        all_wait_times = []
        
        for stop_id, wait_times in self.passenger_wait_times.items():
            all_wait_times.extend(list(wait_times))
            
        return np.mean(all_wait_times) if all_wait_times else 0.0
    
    def _calculate_headway_regularity(self) -> float:
        """Calculates the headway regularity metric."""
        # Simplified implementation
        return 0.8  # Return a default value
    
    def _calculate_passenger_satisfaction(self) -> float:
        """Calculates the composite passenger satisfaction index."""
        satisfaction_factors = []
        
        # 1. Wait time factor
        avg_wait = self._calculate_average_wait_time()
        wait_satisfaction = max(0, 1 - (avg_wait / 1200))  # 0 satisfaction for a 20-minute wait
        satisfaction_factors.append(wait_satisfaction * 0.4)  # 40% weight
        
        # 2. Boarding success rate factor
        total_success = 0
        total_attempts = 0
        for stop_id, stats in self.boarding_success_rate.items():
            total_success += stats['success']
            total_attempts += stats['total']
            
        boarding_rate = total_success / total_attempts if total_attempts > 0 else 1.0
        satisfaction_factors.append(boarding_rate * 0.3)  # 30% weight
        
        # 3. On-time performance factor
        otp = self._calculate_overall_otp()
        satisfaction_factors.append(otp * 0.3)  # 30% weight
        
        return sum(satisfaction_factors)
    
    def _calculate_system_efficiency(self) -> float:
        """Calculates the system efficiency metric."""
        # Active vehicle ratio
        total_buses = len(self.bus_states)
        active_buses = sum(1 for state in self.bus_states.values() 
                          if state in ['running', 'at_stop'])
        utilization = active_buses / total_buses if total_buses > 0 else 0.0
        
        # Congested stop ratio (fewer is better)
        total_stops = len(self.stop_queues)
        congested_stops = sum(1 for queue in self.stop_queues.values() if queue > 20)
        congestion_factor = 1 - (congested_stops / total_stops if total_stops > 0 else 0)
        
        # Composite efficiency index
        efficiency = (utilization * 0.6 + congestion_factor * 0.4)
        
        return efficiency
    
    def _format_kpi_summary(self) -> str:
        """Formats the KPI summary."""
        if not self.current_kpis:
            return "No KPI data"
            
        summary = []
        if 'otp' in self.current_kpis:
            summary.append(f"OTP={self.current_kpis['otp']:.2%}")
        if 'avg_wait_time' in self.current_kpis:
            summary.append(f"Avg Wait={self.current_kpis['avg_wait_time']:.1f}s")
        if 'passenger_satisfaction' in self.current_kpis:
            summary.append(f"Passenger Satisfaction={self.current_kpis['passenger_satisfaction']:.2f}")
            
        return ", ".join(summary)
    
    def export_kpis(self, filename: Optional[str] = None):
        """Exports KPI data to a file."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"logs/kpi_report_{timestamp}.json"
            
        # Ensure the directory exists
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # Prepare data for export
        export_data = {
            'simulation_time': self.env.now,
            'export_time': datetime.now().isoformat(),
            'current_kpis': self.current_kpis,
            'kpi_history': {
                name: history[-100:] for name, history in self.kpi_history.items()  # Last 100 data points
            }
        }
        
        # Write to file
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, ensure_ascii=False, indent=2)
            logger.info(f"KPI data exported to: {filename}")
        except Exception as e:
            logger.error(f"Failed to export KPI data: {e}") 