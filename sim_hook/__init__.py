"""
SimPy data collection hook module
Provides real-time data collection and event handling functionality
"""

from .hook import SimPyDataHook, BusEvent
from .integration_example import EnhancedBus, EnhancedEventHandler, integrate_data_hook

__version__ = "0.1.0"
__all__ = [
    "SimPyDataHook",
    "BusEvent", 
    "EnhancedBus",
    "EnhancedEventHandler",
    "integrate_data_hook"
] 