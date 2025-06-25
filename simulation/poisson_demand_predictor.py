"""
Poisson-based demand prediction module for adaptive headway scheduling.
Uses historical arrival rates directly to predict future demand with Poisson process.
"""
from typing import Dict, List, Tuple, Optional
import numpy as np
from datetime import datetime
import json
import logging

logger = logging.getLogger(__name__)


class PoissonDemandPredictor:
    """
    Demand predictor based on the Poisson process, using historical arrival rate data directly.
    """
    
    def __init__(self, route_id: str = "601001"):
        """
        Initializes the Poisson demand predictor.
        
        Args:
            route_id: The route ID (used to select the corresponding route from the arrival_rate data).
        """
        self.route_id = route_id
        # Historical arrival rate data: {stop_id: {month: {day: {hour: rate}}}}
        self.arrival_rates: Dict[str, Dict[str, Dict[str, Dict[str, float]]]] = {}
        
    def load_historical_data(self, arrival_rates_file: str):
        """
        Loads historical arrival rate data.
        """
        try:
            with open(arrival_rates_file, 'r', encoding='utf-8') as f:
                all_data = json.load(f)
                
            # Extract data for the corresponding route
            if self.route_id in all_data:
                self.arrival_rates = all_data[self.route_id]
                logger.info(f"Loaded arrival rates for route {self.route_id} with {len(self.arrival_rates)} stops")
            else:
                logger.warning(f"Route {self.route_id} not found in arrival rates file")
                
        except Exception as e:
            logger.error(f"Failed to load historical data: {e}")
            
    def get_arrival_rate(self, stop_id: str, current_time: float) -> float:
        """
        Gets the historical arrival rate for a specific stop at the current time.
        
        Args:
            stop_id: The stop ID.
            current_time: The current timestamp (in seconds).
            
        Returns:
            The arrival rate (passengers/hour).
        """
        # Convert the timestamp to a datetime object
        dt = datetime.fromtimestamp(current_time)
        month = str(dt.month)
        day = str(dt.day)
        hour = str(dt.hour)
        
        # Try to get the corresponding arrival rate
        try:
            if (stop_id in self.arrival_rates and 
                month in self.arrival_rates[stop_id] and
                day in self.arrival_rates[stop_id][month] and
                hour in self.arrival_rates[stop_id][month][day]):
                
                rate = self.arrival_rates[stop_id][month][day][hour]
                return float(rate)
            else:
                # If there is no exact match, try to use the average value for that stop and hour
                return self._get_hourly_average(stop_id, dt.hour)
        except Exception as e:
            logger.warning(f"Error getting arrival rate for stop {stop_id}: {e}")
            return 0.1  # Default minimum arrival rate
            
    def _get_hourly_average(self, stop_id: str, hour: int) -> float:
        """
        Gets the average arrival rate for a specific stop at a specific hour.
        """
        if stop_id not in self.arrival_rates:
            return 0.1
            
        hour_str = str(hour)
        rates = []
        
        # Iterate through all months and days to collect the arrival rates for that hour
        for month_data in self.arrival_rates[stop_id].values():
            for day_data in month_data.values():
                if hour_str in day_data:
                    rates.append(float(day_data[hour_str]))
                    
        if rates:
            return sum(rates) / len(rates)
        else:
            return 0.1  # Default minimum arrival rate
            
    def predict_demand(self, stop_ids: List[str], current_time: float,
                      prediction_window: float) -> float:
        """
        Predicts the total demand for a specified set of stops within a future time window.
        Uses a Poisson process to calculate the expected number of arrivals.
        
        Args:
            stop_ids: A list of stop IDs to predict for.
            current_time: The current timestamp.
            prediction_window: The prediction time window (in seconds).
            
        Returns:
            The predicted total demand (expected number of passengers).
        """
        total_expected_passengers = 0.0
        
        for stop_id in stop_ids:
            # Get the arrival rate for this stop (passengers/hour)
            arrival_rate_per_hour = self.get_arrival_rate(stop_id, current_time)
            
            # Convert to arrival rate within the prediction window (passengers/second)
            arrival_rate_per_second = arrival_rate_per_hour / 3600.0
            
            # Use a Poisson process to calculate the expected number of passengers
            # E[N(t)] = λ * t
            expected_passengers = arrival_rate_per_second * prediction_window
            
            total_expected_passengers += expected_passengers
            
        return total_expected_passengers
    
    def predict_demand_with_time_varying_rate(self, stop_ids: List[str], 
                                            current_time: float,
                                            prediction_window: float,
                                            time_step: float = 300) -> float:
        """
        Demand prediction that considers time-varying arrival rates.
        Divides the prediction window into smaller segments and uses the arrival rate corresponding to the time of each segment.
        
        Args:
            stop_ids: A list of stop IDs to predict for.
            current_time: The current timestamp.
            prediction_window: The prediction time window (in seconds).
            time_step: The time step (in seconds), default is 5 minutes.
            
        Returns:
            The predicted total demand (expected number of passengers).
        """
        total_expected_passengers = 0.0
        
        # Divide the prediction window into segments
        num_steps = int(prediction_window / time_step)
        if num_steps == 0:
            num_steps = 1
            time_step = prediction_window
            
        for stop_id in stop_ids:
            stop_total = 0.0
            
            for i in range(num_steps):
                # Calculate the midpoint time of this segment
                segment_time = current_time + i * time_step + time_step / 2
                
                # Get the arrival rate for this time segment
                arrival_rate_per_hour = self.get_arrival_rate(stop_id, segment_time)
                arrival_rate_per_second = arrival_rate_per_hour / 3600.0
                
                # Calculate the expected number of passengers for this segment
                segment_passengers = arrival_rate_per_second * time_step
                stop_total += segment_passengers
                
            total_expected_passengers += stop_total
            
        return total_expected_passengers
    
    def simulate_poisson_arrivals(self, stop_id: str, current_time: float,
                                duration: float, seed: Optional[int] = None) -> List[float]:
        """
        Simulates the Poisson arrival process (for testing and validation only, not for actual prediction).
        
        Args:
            stop_id: The stop ID.
            current_time: The start time.
            duration: The simulation duration (in seconds).
            seed: The random seed.
            
        Returns:
            A list of arrival times.
        """
        if seed is not None:
            np.random.seed(seed)
            
        arrival_times = []
        t = 0
        
        while t < duration:
            # Get the arrival rate for the current time
            arrival_rate_per_hour = self.get_arrival_rate(stop_id, current_time + t)
            arrival_rate_per_second = arrival_rate_per_hour / 3600.0
            
            if arrival_rate_per_second > 0:
                # Generate arrival intervals from an exponential distribution
                interval = np.random.exponential(1.0 / arrival_rate_per_second)
                t += interval
                
                if t < duration:
                    arrival_times.append(t)
            else:
                # If the arrival rate is 0, skip some time
                t += 300  # Skip 5 minutes
                
        return arrival_times 