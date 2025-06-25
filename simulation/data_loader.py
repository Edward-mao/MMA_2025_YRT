"""
Data loading utilities for the bus simulation system.
Handles loading and validation of JSON data files.
"""
import json
import os
import sys
from typing import Dict, Any, Optional, List
from .logger_config import get_logger
from . import paths

logger = get_logger(__name__)

# Ensure script can find simulation package
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Helper function to construct absolute path relative to this file
def get_absolute_data_path(relative_path: str) -> str:
    script_dir = os.path.dirname(__file__)
    # Adjust path based on config file location relative to data
    # In our case, config.py is inside 'simulation', and data is one level up
    # So we need to go up one level from script_dir
    base_dir = os.path.dirname(script_dir)
    return os.path.join(base_dir, relative_path.lstrip('../'))

def load_json_data(file_path: str) -> Dict[str, Any] | None:
    """
    Load data from a JSON file with specified encoding.
    
    Args:
        file_path: Path to the JSON file.
        
    Returns:
        A dictionary with the loaded data, or None on error.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ERROR: File not found at {file_path}")
    except json.JSONDecodeError:
        print(f"ERROR: Could not decode JSON from {file_path}")
    except Exception as e:
        print(f"An unexpected error occurred while loading {file_path}: {e}")
    return None

def load_stop_mapping(scenario_name: str) -> Dict[str, str]:
    """Load the SimPy to SUMO stop ID mapping."""
    # Construct path directly based on the scenario name
    # This avoids relying on a generic `get_path` that doesn't exist.
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    mapping_file = os.path.join(project_root, scenario_name, 'stop_mapping.json')
    
    try:
        with open(mapping_file, 'r') as f:
            data = json.load(f)
            
            # Handle the new bidirectional mapping format
            if 'simpy_to_sumo' in data and isinstance(data['simpy_to_sumo'], dict):
                if 'northbound' in data['simpy_to_sumo'] and 'southbound' in data['simpy_to_sumo']:
                    # New bidirectional mapping format
                    return data
                else:
                    # Old format but under simpy_to_sumo
                    return {
                        'simpy_to_sumo': {
                            'northbound': {str(k): str(v) for k, v in data.get('simpy_to_sumo', {}).items()},
                            'southbound': {}  # Old format has no southbound mapping
                        },
                        'sumo_routes': {}
                    }
            else:
                # Completely old format, direct key-value mapping
                return {
                    'simpy_to_sumo': {
                        'northbound': {str(k): str(v) for k, v in data.items()},
                        'southbound': {}  # Old format has no southbound mapping
                    },
                    'sumo_routes': {}
                }
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Failed to load or parse stop mapping from {mapping_file}: {e}")
        return {
            'simpy_to_sumo': {
                'northbound': {},
                'southbound': {}
            },
            'sumo_routes': {}
        }
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading stop mapping: {e}")
        return {
            'simpy_to_sumo': {
                'northbound': {},
                'southbound': {}
            },
            'sumo_routes': {}
        }

def get_arrival_rate(arrival_data: Dict[str, Any], route_id: str, stop_id: str, month: int, day: int, period: str) -> float | None:
    """Retrieves the arrival rate (lambda) for a specific stop and time."""
    try:
        # JSON keys are strings, so convert all parts of the key to string for lookup.
        # This structure must match the arrival_rate.json file.
        return arrival_data[str(route_id)][str(stop_id)][str(month)][str(day)][period]
    except KeyError:
        # This warning is kept because it's useful to know if certain specific rates are missing from the data file.
        print(f"Warning: Arrival rate not found for route {route_id}, stop {stop_id}, month {month}, day {day}, period {period}. Returning None.")
        return None

def get_destination_weights(weight_data: Dict[str, Any], route_id: str, month: int, day: int, period: str) -> List[float] | None:
    """Retrieves the destination weight vector for a specific route and time."""
    try:
        # Navigate the new structure: Route -> Month -> Day -> Period
        # Try accessing with string keys first
        return weight_data[str(route_id)][str(month)][str(day)][period]
    except KeyError:
        try:
            # Fallback for integer keys
            return weight_data[int(route_id)][int(month)][int(day)][period]
        except (KeyError, ValueError):
            print(f"Warning: Destination weights not found for route {route_id}, month {month}, day {day}, period {period}. Returning None.")
            return None

def get_interchange_status(interchange_data: Dict[str, int], stop_id: str) -> int:
    """Checks if a stop is an interchange station."""
    return interchange_data.get(stop_id, 0) # Default to 0 (non-interchange) if not found

def load_stop_data(file_path: str) -> Dict[str, Any] | None:
    """
    Load stop data, which includes stop locations, routes, and travel times.
    This function can be expanded to handle more complex data formats.
    """
    return load_json_data(file_path)

def extract_route_stops(routes_data: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Extracts and sorts stop IDs for each route from the raw route data.
    
    Args:
        routes_data: The 'routes' section from the stops data file.
        
    Returns:
        A dictionary mapping route_id to an ordered list of stop_ids.
    """
    processed_routes = {}
    for route_id, route_info in routes_data.items():
        if 'stops' in route_info and isinstance(route_info['stops'], list):
            # Sort stops by sequence number and extract stop_id
            sorted_stops = sorted(route_info['stops'], key=lambda x: x.get('sequence', float('inf')))
            processed_routes[route_id] = [stop['stop_id'] for stop in sorted_stops if 'stop_id' in stop]
        else:
            # Handle old format or malformed data
            if isinstance(route_info, list):
                 processed_routes[route_id] = route_info
            else:
                 processed_routes[route_id] = []
    return processed_routes

def save_json_data(data: Dict[str, Any], file_path: str):
    """
    Save data to a JSON file with specified encoding.
    
    Args:
        data: Data to save.
        file_path: Path to the output file.
    """
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Data successfully saved to {file_path}")
    except Exception as e:
        print(f"An error occurred while saving data to {file_path}: {e}")


