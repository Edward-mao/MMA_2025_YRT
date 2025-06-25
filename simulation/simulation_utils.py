import datetime
from typing import Tuple, List, Any
import numpy as np
import os
import sys

# Ensure script can find simulation package
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

try:
    from simulation.config import TIME_PERIODS
except ImportError:
    from config import TIME_PERIODS

def format_time(seconds: float) -> str:
    """Converts seconds into a human-readable HH:MM:SS format."""
    return str(datetime.timedelta(seconds=int(seconds)))

def get_current_period(seconds_into_day: float) -> str | None:
    """
    Determines the time period (e.g., '1') based on the seconds into the day.
    
    Args:
        seconds_into_day: The number of seconds elapsed since midnight.
        
    Returns:
        The name of the period, or None if not found.
    """
    for period, (start, end) in TIME_PERIODS.items():
        if start <= seconds_into_day < end:
            return period
    return None

def weighted_sampling(probabilities: List[float], items: List[Any]) -> Any:
    """
    Performs weighted sampling from a list of items based on probabilities.
    
    Args:
        probabilities: A list of probabilities corresponding to the items.
        items: A list of items to sample from.
        
    Returns:
        A randomly selected item based on the weights.
    """
    return np.random.choice(items, p=probabilities)
