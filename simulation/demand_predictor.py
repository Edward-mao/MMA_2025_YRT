"""
Demand prediction module for adaptive headway scheduling.
Implements EWMA (Exponential Weighted Moving Average) prediction.
"""
from typing import Dict, List, Tuple, Optional
import numpy as np
from collections import defaultdict, deque
from datetime import datetime, timedelta
import json
import logging

logger = logging.getLogger(__name__)


class DemandPredictor:
    """
    Demand predictor that uses EWMA for short-term demand forecasting.
    """
    
    def __init__(self, alpha: float = 0.3, window_size: int = 5, 
                 time_resolution_min: int = 5):
        """
        Initializes the demand predictor.
        
        Args:
            alpha: EWMA smoothing factor (0-1).
            window_size: Sliding window size (minutes).
            time_resolution_min: Time resolution (minutes).
        """
        self.alpha = alpha
        self.window_size = window_size * 60  # Convert to seconds
        self.time_resolution = time_resolution_min * 60
        
        # Historical demand data: {stop_id: {time_slot: demand_rate}}
        self.historical_demand: Dict[str, Dict[int, float]] = defaultdict(dict)
        
        # Real-time demand data: {stop_id: deque[(timestamp, count)]}
        self.realtime_demand: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=100)
        )
        
        # EWMA value cache: {stop_id: {time_slot: ewma_value}}
        self.ewma_cache: Dict[str, Dict[int, float]] = defaultdict(dict)
        
    def load_historical_data(self, arrival_rates_file: str):
        """
        Loads historical arrival rate data.
        """
        try:
            with open(arrival_rates_file, 'r') as f:
                data = json.load(f)
                
            # Process nested data format
            # Format: {route_id: {stop_id: {month: {day: {hour: rate}}}}}
            for route_id, route_data in data.items():
                if isinstance(route_data, dict):
                    for stop_id, stop_data in route_data.items():
                        if isinstance(stop_data, dict):
                            # Aggregate data for all months and days
                            hourly_rates = defaultdict(list)
                            
                            for month, month_data in stop_data.items():
                                if isinstance(month_data, dict):
                                    for day, day_data in month_data.items():
                                        if isinstance(day_data, dict):
                                            for hour, rate in day_data.items():
                                                try:
                                                    hour_int = int(hour)
                                                    rate_float = float(rate)
                                                    hourly_rates[hour_int].append(rate_float)
                                                except (ValueError, TypeError):
                                                    continue
                            
                            # Calculate the average arrival rate for each hour
                            for hour, rates in hourly_rates.items():
                                if rates:
                                    avg_rate = sum(rates) / len(rates)
                                    # Convert to arrival rate per second
                                    time_slot = hour * 3600
                                    self.historical_demand[stop_id][time_slot] = avg_rate / 3600
                                    
            logger.info(f"Loaded historical demand data for {len(self.historical_demand)} stops")
        except Exception as e:
            logger.error(f"Failed to load historical data: {e}")
            
    def _time_str_to_seconds(self, time_str: str) -> int:
        """
        Converts HH:MM format to seconds into the day.
        """
        try:
            h, m = map(int, time_str.split(':'))
            return h * 3600 + m * 60
        except:
            return 0
            
    def get_time_slot(self, timestamp: float) -> int:
        """
        Gets the time slot corresponding to the timestamp.
        """
        # Convert to seconds into the day
        seconds_in_day = int(timestamp) % 86400
        # Align to the time resolution
        return (seconds_in_day // self.time_resolution) * self.time_resolution
        
    def update_realtime_demand(self, stop_id: str, timestamp: float, 
                              passenger_count: int = 1):
        """
        Updates real-time demand data.
        """
        self.realtime_demand[stop_id].append((timestamp, passenger_count))
        
    def predict_demand(self, stop_ids: List[str], current_time: float,
                      prediction_window: float) -> float:
        """
        Predicts the total demand rate for a specified set of stops within a future time window.
        
        Args:
            stop_ids: A list of stop IDs to predict for.
            current_time: The current timestamp.
            prediction_window: The prediction time window (in seconds).
            
        Returns:
            The predicted total demand rate (passengers/second).
        """
        total_demand = 0.0
        
        for stop_id in stop_ids:
            # Get the predicted demand rate for this stop
            demand_rate = self._predict_stop_demand(stop_id, current_time)
            total_demand += demand_rate
            
        return total_demand
        
    def _predict_stop_demand(self, stop_id: str, current_time: float) -> float:
        """
        Predicts the demand rate for a single stop.
        """
        time_slot = self.get_time_slot(current_time)
        
        # 1. Get the historical baseline value
        historical_rate = self.historical_demand.get(stop_id, {}).get(
            time_slot, 0.1  # Default minimum demand rate
        )
        
        # 2. Calculate the real-time EWMA
        realtime_rate = self._calculate_realtime_ewma(stop_id, current_time)
        
        # 3. If there is real-time data, use EWMA fusion; otherwise, use historical data
        if realtime_rate is not None:
            # Update the EWMA cache
            if stop_id not in self.ewma_cache:
                self.ewma_cache[stop_id][time_slot] = historical_rate
                
            prev_ewma = self.ewma_cache[stop_id].get(time_slot, historical_rate)
            new_ewma = self.alpha * realtime_rate + (1 - self.alpha) * prev_ewma
            self.ewma_cache[stop_id][time_slot] = new_ewma
            
            return new_ewma
        else:
            return historical_rate
            
    def _calculate_realtime_ewma(self, stop_id: str, 
                                current_time: float) -> Optional[float]:
        """
        Calculates the EWMA for real-time data.
        """
        if stop_id not in self.realtime_demand:
            return None
            
        recent_data = self.realtime_demand[stop_id]
        if not recent_data:
            return None
            
        # Only consider data within the time window
        window_start = current_time - self.window_size
        valid_data = [(t, c) for t, c in recent_data if t >= window_start]
        
        if not valid_data:
            return None
            
        # Calculate the average arrival rate within the time window
        total_passengers = sum(c for _, c in valid_data)
        time_span = current_time - valid_data[0][0]
        
        if time_span > 0:
            return total_passengers / time_span
        else:
            return None
            
    def get_demand_forecast(self, stop_id: str, start_time: float,
                           end_time: float, resolution: int = 300) -> List[Tuple[float, float]]:
        """
        Gets the demand forecast sequence for a specified time period.
        
        Returns:
            [(timestamp, demand_rate), ...]
        """
        forecast = []
        current = start_time
        
        while current <= end_time:
            rate = self._predict_stop_demand(stop_id, current)
            forecast.append((current, rate))
            current += resolution
            
        return forecast 