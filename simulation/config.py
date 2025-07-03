import os

# Default logging configuration
LOG_LEVEL = "INFO"  # Log level, can be set to DEBUG/INFO/WARNING/ERROR/CRITICAL
LOG_TO_CONSOLE = False  # Whether to output to the console
LOG_TO_FILE = True     # Whether to output to a file
LOGS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "logs")

REGULAR_BOARDING_TIME = 2.0  # Time for each regular passenger to board (seconds)
DISABLED_BOARDING_TIME = 45.0  # Time for each disabled passenger to board (seconds)

# New: Alighting time constants
REGULAR_ALIGHTING_TIME = 1.0  # Time for each regular passenger to alight (seconds)
DISABLED_ALIGHTING_TIME = 45.0  # Time for each disabled passenger to alight (seconds)

# Vehicle capacity constants
REGULAR_CAPACITY_COST = 1            # Regular passenger occupies 1 capacity unit
DISABLED_CAPACITY_COST = 2           # Disabled passenger (e.g., wheelchair) occupies 2 capacity units
BUS_CAPACITY = 75                    # Maximum vehicle capacity (in capacity units, not seats)
MAX_DISABLED_PASSENGERS_PER_BUS = 1  # Maximum number of disabled passengers allowed per bus

# Passenger generation related
DISABLED_PASSENGER_PROBABILITY = 0.01  # 1% of passengers are disabled
REQUEUE_PROPORTION = 1.0               # Probability of requeuing after being denied boarding due to full capacity

# Scheduling and Timetable
DISPATCH_INTERVAL_SECONDS = 1800        # Default dispatch interval without a timetable (seconds)
TIMETABLE_FILE = "Timetable/default_timetable.json"  # Default timetable file path (if not provided externally)

# Dwell Time
FIXED_DWELL_TIME = 3.0  # Fixed dwell time at each stop (seconds), for simulating basic operations like opening/closing doors

# Vehicle travel parameters (units: m/s, m/s²)
# MAX_SPEED = 15.0                # Approx. 54 km/h
# ACCELERATION = 1.0              # Acceleration
# DECELERATION = 1.0              # Deceleration

# The following three items are dynamically read from vtypes.xml by simulation.bus and injected at runtime
# MAX_SPEED
# ACCELERATION
# DECELERATION

# Failure related
MEAN_TIME_BETWEEN_FAILURES = 21600  # Mean time between failures (seconds), default 6 hours

# Travel time estimation helper
DEFAULT_DISTANCE_METERS = 1000.0            # Default distance between adjacent stops used temporarily
TRAVEL_TIME_FALLBACK_SPEED_FACTOR = 0.8     # Fallback speed factor (used when travel time calculation fails)

# Other parameters
MAX_BOARDING_ITERATIONS = 10  # Safety limit for the boarding loop to prevent infinite loops

# ------------------ Time Periods ------------------
# Used to look up arrival rates/weights based on different periods of the day.
# Here is a simplified 6-period division, which can be adjusted based on actual data files.
# "period": (start_second_in_day, end_second_in_day)
TIME_PERIODS = {
    "0": (0, 21600),                 # 00:00 – 06:00
    "1": (21600, 32400),           # 06:00 – 09:00
    "2": (32400, 54000),   # 09:00 – 15:00
    "3": (54000, 68400),  # 15:00 – 19:00
    "4": (68400, 79200),  # 19:00 – 22:00
    "5": (79200, 86400),  # 22:00 – 24:00
}
