import pandas as pd
from typing import List, Dict, Any

def calculate_deviation(actual: float, forecast: float) -> float:
    """
    Calculate the deviation between actual and forecast energy values.
    
    Args:
        actual: Actual energy consumption/generation in kWh
        forecast: Forecasted energy consumption/generation in kWh
        
    Returns:
        Deviation in kWh (actual - forecast)
    """
    return actual - forecast

def calculate_deviation_percentage(actual: float, forecast: float) -> float:
    """
    Calculate the deviation as a percentage of the forecast.
    
    Args:
        actual: Actual energy consumption/generation in kWh
        forecast: Forecasted energy consumption/generation in kWh
        
    Returns:
        Deviation as a percentage of forecast
    """
    if forecast == 0:
        return 0.0 if actual == 0 else float('inf') if actual > 0 else float('-inf')
    return ((actual - forecast) / forecast) * 100

class DeviationAnalyzer:
    """
    Analyzes deviations for a portfolio of metering points over time.
    """
    def __init__(self, readings_data: List[Dict[str, Any]], forecast_data: List[Dict[str, Any]]):
        """
        Initializes the analyzer with readings and forecast data.

        Args:
            readings_data: A list of dictionaries, each representing a meter reading.
                           Expected keys: 'metering_point_id', 'timestamp', 'value_kwh'.
            forecast_data: A list of dictionaries, each representing a forecast.
                           Expected keys: 'metering_point_id', 'timestamp', 'value_kwh'.
        """
        self.readings_df = self._prepare_data(readings_data)
        self.forecast_df = self._prepare_data(forecast_data)
        self.merged_df = pd.DataFrame()

    def _prepare_data(self, data: List[Dict[str, Any]]) -> pd.DataFrame:
        """Converts raw data into a pandas DataFrame with a datetime index."""
        if not data:
            return pd.DataFrame(columns=['metering_point_id', 'value_kwh']).set_index(pd.to_datetime([]))
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp')
        return df

    def calculate_portfolio_deviation(self) -> pd.DataFrame:
        """
        Calculates the total deviation for the entire portfolio over time.

        Returns:
            A pandas DataFrame with the aggregated actuals, forecasts, and deviations.
        """
        # Aggregate readings and forecasts by timestamp
        actuals_agg = self.readings_df.groupby('timestamp')['value_kwh'].sum()
        forecast_agg = self.forecast_df.groupby('timestamp')['value_kwh'].sum()

        # Merge aggregated data
        self.merged_df = pd.DataFrame({'actual_kwh': actuals_agg, 'forecast_kwh': forecast_agg}).fillna(0)
        
        # Calculate deviation
        self.merged_df['deviation_kwh'] = self.merged_df['actual_kwh'] - self.merged_df['forecast_kwh']
        
        return self.merged_df

    def get_top_contributors(self, n: int = 5) -> Dict[str, float]:
        """
        Identifies the top N metering points contributing to the total deviation.

        Args:
            n: The number of top contributors to return.

        Returns:
            A dictionary with metering point IDs and their total deviation in kWh.
        """
        # Merge individual readings and forecasts
        individual_df = pd.merge(
            self.readings_df,
            self.forecast_df,
            on=['timestamp', 'metering_point_id'],
            suffixes=('_actual', '_forecast'),
            how='outer'
        ).fillna(0)

        # Calculate individual deviation
        individual_df['deviation_kwh'] = individual_df['value_kwh_actual'] - individual_df['value_kwh_forecast']
        
        # Sum absolute deviation per metering point
        deviation_by_meter = individual_df.groupby('metering_point_id')['deviation_kwh'].apply(lambda x: x.abs().sum())
        
        # Get top N contributors
        top_contributors = deviation_by_meter.nlargest(n)
        
        return top_contributors.to_dict()
