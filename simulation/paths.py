#!/usr/bin/env python
"""
Path Management Module
Centralize management of paths in the project, ensuring code portability
"""
import os
import sys

# Get project root directory (parent directory of paths.py in the simulation directory)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Ensure project root directory is in Python path
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Main directories
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
SIMULATION_DIR = os.path.join(PROJECT_ROOT, 'simulation')
LOGS_DIR = os.path.join(PROJECT_ROOT, 'logs')

# Data files
STOPS_FILE = os.path.join(DATA_DIR, 'stops.json')
ARRIVAL_RATES_FILE = os.path.join(DATA_DIR, 'arrival_rates.json')
DESTINATION_WEIGHTS_FILE = os.path.join(DATA_DIR, 'destination_weights.json')
INTERCHANGE_STATIONS_FILE = os.path.join(DATA_DIR, 'interchange_stations.json')
CONFIG_FILE = os.path.join(DATA_DIR, 'simulation_config.json')


def ensure_directories():
    """Ensure all necessary directories exist"""
    directories = [DATA_DIR, SIMULATION_DIR, LOGS_DIR]
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Created directory: {directory}")


def get_relative_path(absolute_path):
    """Convert absolute path to a path relative to the project root directory"""
    try:
        return os.path.relpath(absolute_path, PROJECT_ROOT)
    except ValueError:
        # If the path is not on the same drive, return the absolute path
        return absolute_path


def resolve_path(path):
    """Resolve path: if relative, resolve relative to the project root directory"""
    if os.path.isabs(path):
        return path
    return os.path.join(PROJECT_ROOT, path)


# Path function for backward compatibility
def get_data_path(filename):
    """Get the full path of a file in the data directory"""
    return os.path.join(DATA_DIR, filename)


def get_logs_path(filename):
    """Get the full path of a file in the logs directory"""
    return os.path.join(LOGS_DIR, filename)


if __name__ == "__main__":
    # Test paths
    print("=" * 70)
    print("Path Management System Test")
    print("=" * 70)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Data directory: {DATA_DIR}")
    print(f"Simulation directory: {SIMULATION_DIR}")
    print(f"Log directory: {LOGS_DIR}")
    print()
    print("Main data file paths:")
    print(f"Stops file: {STOPS_FILE}")
    print(f"Arrival rates file: {ARRIVAL_RATES_FILE}")
    print(f"Destination weights file: {DESTINATION_WEIGHTS_FILE}")
    print(f"Interchange stations file: {INTERCHANGE_STATIONS_FILE}")
    print()
    print("Relative path examples:")
    print(f"Stops file relative path: {get_relative_path(STOPS_FILE)}")
    print()
    
    # Check if directories exist
    print("Directory existence check:")
    for name, path in [("Data", DATA_DIR), ("Log", LOGS_DIR)]:
        exists = "Exists" if os.path.exists(path) else "Does not exist"
        print(f"  {name} directory: {exists}")
    print("=" * 70) 