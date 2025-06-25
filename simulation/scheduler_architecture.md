# Scheduler Architecture Design Document

## Overview

This document describes the new scheduler architecture, which decouples the scheduling logic from the rest of the system, improving its extensibility and maintainability.

## Architectural Components

### 1. Scheduler Interface (scheduler_interface.py)

#### SchedulerInterface (Abstract Base Class)
Defines the interface that all schedulers must implement:

```python
class SchedulerInterface(ABC):
    def schedule_route(route_id, route_stops, ...): 
        """Schedules buses for a specific route."""
        
    def get_next_departure_time(route_id): 
        """Gets the next departure time."""
        
    def update_schedule(route_id, schedule_data): 
        """Updates the dispatch schedule."""
        
    def set_bus_creator(bus_creator): 
        """Sets the bus creation function."""
```

#### TimetableScheduler
An implementation of a timetable-based scheduler:
- Retrieves timetable data from the TimetableManager.
- Schedules buses according to the predefined times.
- Supports dynamic updates to the timetable.

#### IntervalScheduler
An implementation of a fixed-interval scheduler:
- Schedules buses at fixed time intervals.
- Suitable for scenarios without a timetable.
- Supports dynamic adjustments to the dispatch interval.

### 2. Timetable Manager (timetable.py)

#### TimetableManager
Centralizes all timetable-related functionalities:

```python
class TimetableManager:
    def load_timetable_from_file(file_path):
        """Loads a timetable from a file."""
        
    def load_route_timetable(route_id, file_path):
        """Loads a timetable for a specific route."""
        
    def get_schedule_for_route(route_id):
        """Gets the full schedule for a route."""
        
    def get_next_departure_time(route_id, current_time):
        """Gets the next departure time."""
        
    def update_schedule(route_id, new_schedule):
        """Updates the timetable."""
```

Supported timetable formats:
1. Standard format (contains `route_info` and `schedule`).
2. Route ID mapping format.
3. A simple list of schedules for a single route.

### 3. Scheduling Module (scheduling.py)

Retains original functions for backward compatibility while integrating the new scheduler architecture:

- `create_bus_in_sumo()`: A utility function to be called by schedulers.
- `create_scheduler()`: A factory function to create scheduler instances.
- `schedule_bus_dispatch()`: A backward-compatible function.

## Usage Examples

### 1. Using the Timetable Scheduler

```python
# Create a TimetableManager
timetable_manager = TimetableManager()
timetable_manager.load_route_timetable("601001", "601/timetable/601001_timetable.json")

# Create a scheduler
scheduler = TimetableScheduler(env, timetable_manager)
scheduler.set_bus_creator(create_bus_in_sumo)

# Start scheduling
scheduler.schedule_route(
    route_id="601001",
    route_stops=stops,
    bus_stops=bus_stops,
    active_buses_list=active_buses,
    event_handler=event_handler,
    stop_mapping=stop_mapping
)
```

### 2. Using the Interval Scheduler

```python
# Create a scheduler (30-minute interval)
scheduler = IntervalScheduler(env, default_interval=1800)
scheduler.set_bus_creator(create_bus_in_sumo)

# Start scheduling
scheduler.schedule_route(
    route_id="601002",
    route_stops=stops,
    bus_stops=bus_stops,
    active_buses_list=active_buses,
    event_handler=event_handler,
    stop_mapping=stop_mapping,
    interval=1200  # Can override the default interval
)
```

### 3. Dynamically Updating the Schedule

```python
# Update the timetable
new_schedule = [
    {"trip_id": "trip_1", "departure_time": 21600, "stops": [...]},
    {"trip_id": "trip_2", "departure_time": 23400, "stops": [...]}
]
scheduler.update_schedule("601001", new_schedule)

# Update the dispatch interval
scheduler.update_schedule("601002", 900)  # 15-minute interval
```

## Extending with a New Scheduler

To implement a new scheduling strategy, simply:

1. Inherit from `SchedulerInterface`
2. Implement all abstract methods
3. Add the specific scheduling logic

Example: Implementing a dynamic scheduler

```python
class DynamicScheduler(SchedulerInterface):
    """A scheduler that dynamically adjusts frequency based on passenger flow."""
    
    def __init__(self, env, passenger_monitor):
        super().__init__(env)
        self.passenger_monitor = passenger_monitor
        
    def schedule_route(self, route_id, ...):
        # Implement dynamic scheduling logic based on passenger flow
        pass
        
    def get_next_departure_time(self, route_id):
        # Calculate the next departure time based on current passenger flow
        passenger_count = self.passenger_monitor.get_waiting_count(route_id)
        if passenger_count > THRESHOLD:
            return self.env.now + SHORT_INTERVAL
        else:
            return self.env.now + LONG_INTERVAL
```

## Advantages

1. **Decoupling**: Scheduling logic is separated from other system components.
2. **Extensibility**: Easy to add new scheduling strategies.
3. **Maintainability**: Each component has a single responsibility, making it easy to understand and modify.
4. **Backward Compatibility**: Retains original interfaces, not affecting existing code.
5. **Flexibility**: Supports runtime switching and updating of scheduling strategies.

## Migration Guide

Existing code can be migrated gradually:

1. **Phase 1**: Continue using the `schedule_bus_dispatch()` function.
2. **Phase 2**: Directly use the scheduler interface.
3. **Phase 3**: Implement custom schedulers.

Migration Example:

```python
# Old code
schedule_bus_dispatch(env, route_id, stops, interval, ...)

# New code
scheduler = create_scheduler(env, "timetable", timetable_manager=manager)
scheduler.schedule_route(route_id, stops, ...)
``` 