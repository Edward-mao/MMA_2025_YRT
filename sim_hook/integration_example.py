"""
SimPy data collection hook integration example
Show how to integrate data collection hooks into an existing bus simulation system
"""
import sys
sys.path.append('..')

from simulation.bus import Bus
from simulation.bus_stop import BusStop
from simulation.event_handler import EventHandler
from sim_hook.hook import SimPyDataHook
import logging

logger = logging.getLogger(__name__)


class EnhancedBus(Bus):
    """Enhanced Bus class with integrated data collection hook."""
    
    def __init__(self, *args, data_hook: SimPyDataHook = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_hook = data_hook
        self._current_speed = 0
        self._scheduled_times = {}  # Stores scheduled times for each stop
        
    def handle_sumo_stop_arrival(self, stop_id: str):
        """Overrides the stop arrival handling method to add data collection."""
        # Call the parent class method
        super().handle_sumo_stop_arrival(stop_id)
        
        # Data collection
        if self.data_hook:
            try:
                # Get current passenger load
                passenger_load = len(self.passengers)
                wheelchair_count = sum(1 for p in self.passengers if hasattr(p, 'is_wheelchair') and p.is_wheelchair)
                
                # Determine if it is a replacement vehicle
                is_replacement = "_Repl_" in self.bus_id
                
                # Get scheduled time
                scheduled_time = self._scheduled_times.get(stop_id, self.env.now)
                
                # Determine direction (based on route ID)
                direction = "inbound" if "_1" in self.route_id else "outbound"
                
                # Trigger data collection
                self.data_hook.on_bus_arrival(
                    env_time=self.env.now,
                    bus_id=self.bus_id,
                    route_id=self.route_id,
                    stop_id=stop_id,
                    stop_sequence=self.current_stop_index + 1,
                    scheduled_time=scheduled_time,
                    passenger_load=passenger_load,
                    wheelchair_count=wheelchair_count,
                    is_replacement=is_replacement,
                    direction=direction,
                    speed=self._current_speed,
                    distance_to_next=self._calculate_distance_to_next(),
                    distance_to_trip=self._calculate_distance_to_trip()
                )
            except Exception as e:
                logger.error(f"Data collection failed (arrival): {e}")
                
    def _process_stop_arrival(self, bus_stop: BusStop, stop_id: str):
        """Overrides stop processing method to add departure data collection."""
        # Record the load before processing
        initial_load = len(self.passengers)
        
        # Call the parent class method
        result = yield from super()._process_stop_arrival(bus_stop, stop_id)
        
        # Data collection - departure event
        if self.data_hook:
            try:
                # Calculate number of passengers boarded and alighted
                final_load = len(self.passengers)
                alighted = initial_load - final_load + (result[1] if len(result) > 1 else 0)
                boarded = result[1] if len(result) > 1 else 0
                dwell_time = result[2] if len(result) > 2 else 0
                
                # Trigger departure data collection
                self.data_hook.on_bus_departure(
                    env_time=self.env.now,
                    bus_id=self.bus_id,
                    route_id=self.route_id,
                    stop_id=stop_id,
                    boarded=boarded,
                    alighted=alighted,
                    dwell_time=dwell_time,
                    passenger_load=final_load
                )
            except Exception as e:
                logger.error(f"Data collection failed (departure): {e}")
                
        return result
        
    def set_scheduled_times(self, schedule: dict):
        """Sets the scheduled times for each stop."""
        self._scheduled_times = schedule
        
    def _calculate_distance_to_next(self) -> float:
        """Calculates the distance to the next stop."""
        # This needs to be calculated based on actual route data
        # Returning a simulated value for now
        return 500.0  # meters
        
    def _calculate_distance_to_trip(self) -> float:
        """Calculates the distance to the final stop."""
        # This needs to be calculated based on actual route data
        # Returning a simulated value for now
        remaining_stops = len(self.route_stops) - self.current_stop_index - 1
        return remaining_stops * 500.0  # meters


class EnhancedEventHandler(EventHandler):
    """Enhanced EventHandler that supports data collection hooks."""
    
    def __init__(self, env, data_hook: SimPyDataHook = None):
        super().__init__(env)
        self.data_hook = data_hook
        
    def handle_bus_failure(self, failed_bus, all_bus_stops, active_buses_list):
        """Handles vehicle failures and passes the data hook when creating a replacement vehicle."""
        # Import EnhancedBus instead of the original Bus
        replacement_bus_id = f"{failed_bus.bus_id}_Repl_{int(self.env.now)}"
        
        new_bus = EnhancedBus(
            env=self.env,
            bus_id=replacement_bus_id,
            route=failed_bus.route,
            bus_stops=all_bus_stops,
            active_buses_list=active_buses_list,
            event_handler=self,
            data_hook=self.data_hook  # Pass the data hook
        )
        
        active_buses_list.append(new_bus)
        
        # Record the replacement event
        self.record_event('replacement_dispatched', {
            'bus_id': replacement_bus_id,
            'original_bus_id': failed_bus.bus_id,
            'time': self.env.now
        })


def integrate_data_hook(simulation_runner):
    """
    Integrates the data collection hook into an existing SimulationRunner.
    
    Args:
        simulation_runner: The existing SimulationRunner instance.
    """
    # Create the data hook
    data_hook = SimPyDataHook(
        output_dir="./simulation_data",
        batch_size=500,
        log_level="INFO"
    )
    
    # Start the background writer thread
    data_hook.start()
    
    # Replace the event handler
    original_handler = simulation_runner.event_handler
    enhanced_handler = EnhancedEventHandler(simulation_runner.env, data_hook)
    enhanced_handler.event_log = original_handler.event_log  # Keep the original log
    simulation_runner.event_handler = enhanced_handler
    
    # Modify the method for creating a Bus (if possible)
    # This needs to be adjusted based on the actual implementation of SimulationRunner
    
    logger.info("Data collection hook has been integrated into the simulation system")
    
    return data_hook


# Example usage
if __name__ == "__main__":
    # Assuming an existing simulation_runner instance
    # from simulation.simulation_runner import SimulationRunner
    # runner = SimulationRunner(config)
    
    # Integrate the data hook
    # hook = integrate_data_hook(runner)
    
    # Run the simulation
    # runner.run()
    
    # Stop data collection
    # hook.stop()
    
    # View statistics
    # print(hook.get_statistics())
    
    print("Integration example created") 