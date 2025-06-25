# Simplified Adaptive Headway Scheduler User Guide

## Overview

The Simplified Adaptive Headway Scheduler is an intelligent bus dispatching system based on demand forecasting at key stops. By monitoring passenger arrival rates at specific high-demand stops, the system dynamically calculates the optimal dispatch interval and maintains a fixed headway once a vehicle is dispatched, effectively matching capacity with demand.

### Core Features

1.  **[Fixed Monitoring Stops][[memory:677388226891081276]]**: Each route monitors only predefined key stops, simplifying computational complexity.
2.  **Fixed Headway**: Once a vehicle is dispatched, its assigned headway remains constant throughout its trip.
3.  **Simplified Calculation Formula**: Uses a more intuitive formula to calculate the target headway.
4.  **Smart Holding Control**: Applies moderate holding only when a vehicle is running ahead of schedule, avoiding excessive intervention.
5.  **Real-time KPI Tracking**: Monitors system performance indicators to support continuous optimization.

## How It Works

### 1. Core Formula

The target headway is calculated using the following formula:
```
h* = max(h_min, min(h_max, (β* × C) / (Σλ̂/n)))
```

Where:
- `h*`: Target headway (seconds)
- `h_min`: Minimum headway limit
- `h_max`: Maximum headway limit
- `β*`: Target load factor (0.7-1.0)
- `C`: Vehicle capacity
- `Σλ̂`: Total demand rate of all monitored stops (passengers/second)
- `n`: Number of monitored stops

### 2. Monitoring Stop Configuration

The system predefines monitoring stops for each route:

| Route    | Monitored Stops    | Description                |
|----------|--------------------|----------------------------|
| 601001   | 9769, 9770, 9723   | Key high-demand stops for the northbound route |
| 601002   | 9819, 9883         | Key high-demand stops for the southbound route |

### 3. Scheduling Process

#### Dispatch Phase
1.  Collect historical arrival rate data from monitored stops.
2.  Forecast the demand rate for the current period.
3.  Calculate the target headway using the core formula.
4.  Generate a dispatch command, fixing the headway for that vehicle.

#### In-Trip Phase
1.  The vehicle operates according to its fixed headway.
2.  Holding is applied only if the vehicle is running earlier than its scheduled interval.
3.  The holding decision is based on the actual time gap with the preceding vehicle.
4.  The maximum holding time is limited to avoid significant service disruptions.

## Configuration Guide

Add the following configuration to `config.yml`:

```yaml
simpy:
  scheduler:
    type: adaptive_headway       # Set scheduler type
    adaptive_headway:
      beta_target: 1.0           # Target load factor
      bus_capacity: 75           # Vehicle capacity
      h_min: 600                 # Minimum headway (10 minutes)
      h_max: 1800                # Maximum headway (30 minutes)
      max_hold: 30               # Maximum holding time (seconds)
      headway_tolerance: 0.2     # Headway tolerance (20%)
      enable_kpi: true           # Enable KPI tracking
      kpi_export_interval: 3600  # KPI export interval (seconds)
```

### Parameter Tuning Recommendations

| Parameter         | Recommended Range | Description                                           |
|-------------------|-------------------|-------------------------------------------------------|
| `beta_target`     | 0.7-1.0           | Set higher for peak hours, lower for off-peak hours. |
| `h_min`           | 300-600           | Set based on route characteristics and service standards. |
| `h_max`           | 1200-3600         | Prevents excessively long headways during off-peak hours. |
| `max_hold`        | 20-60             | Balances on-time performance with headway maintenance. |
| `headway_tolerance`| 0.1-0.3          | Smaller values for strict control, larger for more flexibility. |

## Usage Steps

### 1. Basic Usage

```bash
# 1. Modify config.yml, set scheduler.type to adaptive_headway
# 2. Run the simulation
python run_simulation_simple.py

# Or use the full simulation runner (includes ETL)
python simulation/simulation_runner.py
```

### 2. Multi-Round Comparative Testing

```bash
# Run the baseline scenario (e.g., using a timetable scheduler)
# Modify config.yml: 
#   scheduler.type: timetable
#   simulation.data_target: baseline
python run_simulation_simple.py

# Run the adaptive scheduler scenario
# Modify config.yml:
#   scheduler.type: adaptive_headway  
#   simulation.data_target: scenario
python run_simulation_simple.py

# Use SQL queries to compare the results
```

### 3. Data Preparation

Ensure the following data files exist and are correctly formatted:
- `601/trail/arrival_rate.json`: Historical arrival rate data (including monitored stops).
- `601/trail/stops.json`: Stop configuration.
- `601/timetable/`: Timetable files (for fallback).

## Performance Monitoring

### KPI Metrics

The system automatically tracks the following key metrics:

1.  **Headway Adjustments**
    - Records the time and duration of each holding action.
    - Statistics on the actual headway distribution.

2.  **Demand Forecast Accuracy**
    - Forecasted vs. actual demand at monitored stops.
    - Analysis of prediction errors.

3.  **Load Factor Distribution**
    - Spatiotemporal distribution of vehicle load factors.
    - Identification of capacity bottlenecks.

### Log Output Example

```
2024-01-15 10:30:45 - ================================================================================
2024-01-15 10:30:45 - Starting Adaptive Headway Scheduler
2024-01-15 10:30:45 -   Route: 601001
2024-01-15 10:30:45 -   Number of stops: 27
2024-01-15 10:30:45 -   Monitored stops: ['9769', '9770', '9723']
2024-01-15 10:30:45 -   Configuration parameters:
2024-01-15 10:30:45 -     - β* (target load factor): 1.0
2024-01-15 10:30:45 -     - h_min (minimum headway): 600s
2024-01-15 10:30:45 -     - h_max (maximum headway): 1800s
2024-01-15 10:30:45 -     - Vehicle capacity: 75
2024-01-15 10:30:45 - ================================================================================

2024-01-15 10:31:00 - Scheduled bus bus_601001_adaptive_1 with FIXED headway 720s
2024-01-15 10:43:00 - Bus bus_601001_adaptive_2 holding for 25s to maintain fixed headway
```

## Advanced Features

### 1. Customizing Monitored Stops

Although the system uses predefined monitored stops, you can customize them by modifying `AdaptiveHeadwayScheduler.MONITORED_STOPS`:

```python
# In adaptive_headway_scheduler.py
MONITORED_STOPS = {
    '601001': ['9769', '9770', '9723', '9777'],  # Add an extra monitored stop
    '601002': ['9819', '9883', '9808']
}
```

### 2. Enhancing Demand Prediction

The system uses a Poisson process for demand prediction, which can be enhanced:

```python
# Adjust the weight of historical data
scheduler.demand_predictor.set_time_window(minutes=10)

# Add special event handling
scheduler.demand_predictor.add_special_event(
    date="2024-01-20",
    multiplier=1.5  # Expected demand is 1.5 times the usual
)
```

### 3. Real-time Monitoring Integration

Access real-time data through the event handler:

```python
# Listen for headway adjustment events in the event handler
def on_headway_adjust(event_data):
    bus_id = event_data['bus_id']
    hold_time = event_data['hold_time']
    # Send to a monitoring system
```

## Troubleshooting

### Common Issues

1.  **Abnormal Headway Calculation**
    - Check if monitored stops exist on the route.
    - Verify the integrity of arrival rate data.
    - Confirm that the vehicle capacity parameter is correct.

2.  **Excessive Holding Times**
    - Decrease the `max_hold` parameter.
    - Increase the `headway_tolerance`.
    - Check if the preceding bus was dispatched correctly.

3.  **Inaccurate Demand Forecasts**
    - Update historical arrival rate data.
    - Adjust the prediction time window.
    - Consider seasonal factors.

### Debugging Tips

1.  Enable DEBUG log level to view detailed calculation processes.
2.  Export KPI data for offline analysis.
3.  Use small-scale tests to validate configuration parameters.

## Best Practices

1.  **Incremental Deployment**: Test on a single route first, then roll out more widely after verifying the results.
2.  **Data Quality Assurance**: Regularly update and validate historical demand data.
3.  **Parameter Optimization**: Continuously optimize parameters based on actual operational data.
4.  **Monitoring and Feedback**: Establish KPI monitoring and alerting mechanisms.

## Comparison with Other Scheduling Strategies

| Feature            | Timetable | Fixed-Interval | Simplified Adaptive |
|--------------------|-----------|----------------|---------------------|
| Flexibility        | Low       | Medium         | High                |
| Implementation Complexity | Low       | Low            | Medium              |
| Demand Responsiveness | Poor      | Fair           | Good                |
| Operational Predictability | High      | High           | Medium              |
| Use Case           | Low-demand routes | High-frequency routes | Routes with fluctuating demand |

## Further Reading

- [Simplified Scheduler Design Document](Documents/SIMPLIFIED_ADAPTIVE_HEADWAY_SCHEDULER.md)
- [Scheduler Architecture Documentation](simulation/scheduler_architecture.md)
- [Poisson Demand Predictor](Documents/POISSON_PREDICTOR_UPDATE.md)

## Changelog

- 2024-01-15: Simplified version release
  - Implemented fixed monitoring stops mechanism.
  - Adopted fixed headway strategy.
  - Simplified calculation formula.
  - Optimized holding control logic.
- 2024-01-10: Initial version
  - Basic adaptive scheduling functionality. 