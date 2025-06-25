"""
Logging configuration for the bus simulation system.
Provides centralized logging setup and utilities.
"""
import logging
import logging.handlers
import os
import sys
from typing import Any, Optional, List, Dict
from datetime import datetime
import threading
import json

# Ensure script can find simulation package
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

try:
    from simulation import paths
    has_paths = True
except ImportError:
    from . import paths
    has_paths = True
except ImportError:
    has_paths = False


def setup_logging():
    """
    Sets up the root logger for the entire application based on config.
    """
    try:
        from simulation import config
    except ImportError:
        # Fallback for when running module directly
        import config

    log_level = getattr(logging, config.LOG_LEVEL.upper(), logging.INFO)
    
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Clear existing handlers
    if root_logger.handlers:
        for handler in root_logger.handlers:
            root_logger.removeHandler(handler)

    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    simple_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )

    # Console handler
    if config.LOG_TO_CONSOLE:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(simple_formatter)
        root_logger.addHandler(console_handler)

    # File handler
    if config.LOG_TO_FILE:
        log_dir = config.LOGS_DIR
        os.makedirs(log_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = os.path.join(log_dir, f'simulation_{timestamp}.log')
        
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(detailed_formatter)
        root_logger.addHandler(file_handler)
        
        root_logger.info(f"Logging to file: {log_file}")

        # --- Create a file-only logger ---
        file_only_logger = logging.getLogger('FileOnlyLogger')
        file_only_logger.setLevel(log_level)
        file_only_logger.addHandler(file_handler)
        file_only_logger.propagate = False # Prevent messages from being passed to the root logger


def get_file_only_logger(name: str) -> logging.Logger:
    """
    Get a logger instance that ONLY logs to the file, not the console.
    """
    # This actually just returns the pre-configured file-only logger.
    # The 'name' argument is kept for API consistency but is not used
    # as we want a single, unified file-only logger instance.
    return logging.getLogger('FileOnlyLogger')


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance. It will inherit configuration from the root logger.
    
    Args:
        name: Logger name (usually __name__ of the module).
        
    Returns:
        Logger instance.
    """
    return logging.getLogger(name)


# Simulation-specific log levels
class SimulationLogLevel:
    """Custom log levels for simulation events."""
    EVENT = 25  # Between INFO and WARNING
    DISPATCH = 26
    FAILURE = 35  # Between WARNING and ERROR


# Add custom levels
logging.addLevelName(SimulationLogLevel.EVENT, "EVENT")
logging.addLevelName(SimulationLogLevel.DISPATCH, "DISPATCH")
logging.addLevelName(SimulationLogLevel.FAILURE, "FAILURE")


# Convenience methods for custom levels
def log_event(logger: logging.Logger, message: str, *args, **kwargs):
    """Log a simulation event."""
    logger.log(SimulationLogLevel.EVENT, message, *args, **kwargs)


def log_dispatch(logger: logging.Logger, message: str, *args, **kwargs):
    """Log a bus dispatch event."""
    logger.log(SimulationLogLevel.DISPATCH, message, *args, **kwargs)


def log_failure(logger: logging.Logger, message: str, *args, **kwargs):
    """Log a failure event."""
    logger.log(SimulationLogLevel.FAILURE, message, *args, **kwargs) 