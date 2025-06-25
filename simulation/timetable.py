# simulation/timetable.py
import json
from typing import Dict, List, Any, Optional
import os
from pathlib import Path
import logging

# Get logger
logger = logging.getLogger(__name__)


class TimetableManager:
    """Timetable manager, responsible for loading, storing, and querying timetable data."""
    
    def __init__(self):
        """Initializes the timetable manager."""
        self.timetables = {}  # Stores timetables for all routes
        self.timetable_files = {}  # Records the timetable file path for each route
        
    def load_timetable_from_file(self, file_path: str) -> Optional[Dict[str, List[Dict[str, Any]]]]:
        """
        Loads timetable data from a JSON file.
        
        Args:
            file_path: The path to the timetable file.
            
        Returns:
            A dictionary containing the route timetables, with the format: {route_id: [trip_data]}
        """
        if not os.path.exists(file_path):
            logger.error(f"Timetable file not found: {file_path}")
            return None
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Failed to read or parse timetable file {file_path}: {e}")
            return None
            
        # Handle different timetable file formats
        result = {}
        
        # Format 1: Standard format with route_info and schedule
        if 'route_info' in data and 'schedule' in data:
            route_id = data['route_info'].get('route_id')
            if route_id:
                schedule = self._process_schedule(data.get('schedule', []))
                result[route_id] = schedule
                
        # Format 2: Direct mapping of route IDs
        elif isinstance(data, dict) and all(isinstance(v, list) for v in data.values()):
            for route_id, schedule in data.items():
                result[route_id] = self._process_schedule(schedule)
                
        # Format 3: A list of schedules for a single route
        elif isinstance(data, list):
            # Try to infer the route ID from the file name
            route_id = self._extract_route_id_from_path(file_path)
            if route_id:
                result[route_id] = self._process_schedule(data)
                
        else:
            logger.warning(f"Unrecognized timetable format: {file_path}")
            
        return result if result else None
        
    def _process_schedule(self, schedule: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Processes raw timetable data to ensure each trip has the necessary fields.
        
        Args:
            schedule: The list of raw timetables.
            
        Returns:
            A list of processed timetables.
        """
        processed_schedule = []
        
        for trip in schedule:
            if not isinstance(trip, dict):
                continue
                
            trip_id = trip.get('trip_id')
            stops = trip.get('stops', [])
            
            # Ensure trip_id exists
            if not trip_id and stops:
                trip_id = f"trip_{len(processed_schedule) + 1}"
                
            if not trip_id or not stops:
                continue
                
            # Get the departure time (scheduled time of the first stop)
            departure_time = trip.get('departure_time')
            if departure_time is None and stops:
                departure_time = stops[0].get('scheduled_time')
                
            if departure_time is None:
                continue
                
            processed_trip = {
                'trip_id': trip_id,
                'departure_time': departure_time,
                'stops': stops
            }
            
            # Keep other potentially useful fields
            for key in ['direction', 'vehicle_type', 'service_type']:
                if key in trip:
                    processed_trip[key] = trip[key]
                    
            processed_schedule.append(processed_trip)
            
        # Sort by departure time
        processed_schedule.sort(key=lambda x: x['departure_time'])
        
        return processed_schedule
        
    def _extract_route_id_from_path(self, file_path: str) -> Optional[str]:
        """Extracts the route ID from the file path."""
        file_name = Path(file_path).stem
        # Try to extract the route ID from the file name (e.g., 601001_timetable.json -> 601001)
        parts = file_name.split('_')
        if parts and parts[0].isdigit():
            return parts[0]
        return None
        
    def load_route_timetable(self, route_id: str, file_path: str) -> bool:
        """
        Loads the timetable for a specific route.
        
        Args:
            route_id: The route ID.
            file_path: The path to the timetable file.
            
        Returns:
            True if loading was successful, False otherwise.
        """
        timetables = self.load_timetable_from_file(file_path)
        if not timetables:
            return False
            
        # Try to find the corresponding route timetable
        if route_id in timetables:
            self.timetables[route_id] = timetables[route_id]
            self.timetable_files[route_id] = file_path
            logger.info(f"Successfully loaded timetable for route {route_id} with {len(self.timetables[route_id])} trips")
            return True
            
        # Try to match by route prefix (e.g., 601 matches 601001)
        route_prefix = route_id.split('_')[0]
        for loaded_route_id, schedule in timetables.items():
            # Check for a bidirectional match: loaded_route_id contains route_id, or route_id contains loaded_route_id
            if loaded_route_id.startswith(route_prefix) or route_id.startswith(loaded_route_id):
                self.timetables[route_id] = schedule
                self.timetable_files[route_id] = file_path
                logger.info(f"Successfully loaded timetable for route {route_id} (matched to {loaded_route_id}), with {len(schedule)} trips")
                return True
                
        logger.warning(f"Timetable for route {route_id} not found in file {file_path}")
        return False
        
    def get_schedule_for_route(self, route_id: str) -> List[Dict[str, Any]]:
        """
        Gets the full timetable for a specified route.
        
        Args:
            route_id: The route ID.
            
        Returns:
            A list of timetables, or an empty list if none is found.
        """
        return self.timetables.get(route_id, [])
        
    def get_next_departure_time(self, route_id: str, current_time: float) -> Optional[float]:
        """
        Gets the next departure time for a specified route.
        
        Args:
            route_id: The route ID.
            current_time: The current simulation time.
            
        Returns:
            The next departure time, or None if there is none.
        """
        schedule = self.get_schedule_for_route(route_id)
        if not schedule:
            return None
            
        for trip in schedule:
            departure_time = trip['departure_time']
            if departure_time > current_time:
                return departure_time
                
        return None
        
    def get_trip_by_time(self, route_id: str, target_time: float, tolerance: float = 60) -> Optional[Dict[str, Any]]:
        """
        Gets the closest trip by time.
        
        Args:
            route_id: The route ID.
            target_time: The target time.
            tolerance: The tolerance (in seconds), trips within this range are acceptable.
            
        Returns:
            The closest trip data, or None if none is found.
        """
        schedule = self.get_schedule_for_route(route_id)
        if not schedule:
            return None
            
        best_trip = None
        min_diff = float('inf')
        
        for trip in schedule:
            departure_time = trip['departure_time']
            diff = abs(departure_time - target_time)
            
            if diff < min_diff and diff <= tolerance:
                min_diff = diff
                best_trip = trip
                
        return best_trip
        
    def update_schedule(self, route_id: str, new_schedule: List[Dict[str, Any]]) -> bool:
        """
        Updates the timetable for a specified route.
        
        Args:
            route_id: The route ID.
            new_schedule: New timetable data.
            
        Returns:
            True if the update was successful, False otherwise.
        """
        try:
            processed_schedule = self._process_schedule(new_schedule)
            self.timetables[route_id] = processed_schedule
            logger.info(f"Successfully updated timetable for route {route_id}, with {len(processed_schedule)} trips")
            return True
        except Exception as e:
            logger.error(f"Failed to update timetable for route {route_id}: {e}")
            return False
            
    def get_all_routes(self) -> List[str]:
        """Gets a list of all route IDs with loaded timetables."""
        return list(self.timetables.keys())
        
    def clear_route_timetable(self, route_id: str) -> None:
        """Clears the timetable for a specified route."""
        if route_id in self.timetables:
            del self.timetables[route_id]
            if route_id in self.timetable_files:
                del self.timetable_files[route_id]
            logger.info(f"Cleared timetable for route {route_id}")


# Keep the original load_timetable function for backward compatibility
def load_timetable(file_path: str) -> Optional[Dict[str, List[Dict[str, Any]]]]:
    """
    Loads timetable data from a JSON file.

    Args:
        file_path: The path to the timetable JSON file.

    Returns:
        A dictionary containing the schedule for each route, or None if loading fails.
        The structure is: { "route_id": [{"trip_id": str, "departure_time": int, "stops": List[Dict]}] }
    """
    manager = TimetableManager()
    return manager.load_timetable_from_file(file_path) 