from typing import List, Dict, Any, Union

def calculate_settlement(deviation_kwh: Union[float, str], price_ct_per_kwh: Union[int, float, str] = 10) -> float:
    """
    Calculate the settlement amount for a deviation in energy consumption/generation.

    Args:
        deviation_kwh: Deviation in kWh (positive = surplus, negative = deficit)
        price_ct_per_kwh: Price per kWh in cents (default: 10 ct/kWh)

    Returns:
        Settlement amount in EUR
    """
    deviation_kwh = float(deviation_kwh)
    price_ct_per_kwh = float(price_ct_per_kwh)
    return (deviation_kwh * price_ct_per_kwh) / 100  # Convert ct to EUR

def calculate_settlement_with_percentage(deviation_kwh: float, forecast_kwh: float, price_ct_per_kwh: int = 10) -> float:
    """
    Calculate the settlement amount and percentage deviation.
    
    Args:
        deviation_kwh: Deviation in kWh (actual - forecast)
        forecast_kwh: Forecasted energy in kWh
        price_ct_per_kwh: Price per kWh in cents (default: 10 ct/kWh)
        
    Returns:
        Settlement amount in EUR
    """
    settlement_eur = calculate_settlement(deviation_kwh, price_ct_per_kwh)
    
    if forecast_kwh == 0:
        deviation_percentage = 0.0 if deviation_kwh == 0 else float('inf') if deviation_kwh > 0 else float('-inf')
    else:
        deviation_percentage = (deviation_kwh / forecast_kwh) * 100
        
    return settlement_eur  # Return only the settlement amount as a float

class SettlementCalculator:
    """
    Calculates settlement for a balance group based on deviations and pricing.
    """
    def __init__(self, default_price_ct_per_kwh: int = 10, default_percentage: int = 100):
        """
        Initializes the calculator with a specific price and percentage.

        Args:
            default_price_ct_per_kwh: The price in cents per kWh for settlement calculations.
            default_percentage: The percentage factor for settlement calculations (default: 100%).
        """
        self.default_price_ct_per_kwh = default_price_ct_per_kwh
        self.default_percentage = default_percentage

    def calculate(self, deviation_kwh: float, price_ct_per_kwh: int = None, percentage: int = None) -> float:
        """
        Calculates the settlement amount for a given deviation.

        Args:
            deviation_kwh: The deviation in kWh.
            price_ct_per_kwh: The price in cents per kWh (optional, defaults to default_price_ct_per_kwh).
            percentage: The percentage factor (optional, defaults to default_percentage).

        Returns:
            The settlement amount in EUR.
        """
        if price_ct_per_kwh is None:
            price_ct_per_kwh = self.default_price_ct_per_kwh
        if percentage is None:
            percentage = self.default_percentage
        effective_price = price_ct_per_kwh * percentage / 100
        return (deviation_kwh * effective_price) / 100

    def calculate_batch(self, deviations: List[float], price_ct_per_kwh: int = None, percentage: int = None) -> List[float]:
        """
        Calculates the settlement amounts for a list of deviations.

        Args:
            deviations: A list of deviations in kWh.
            price_ct_per_kwh: The price in cents per kWh (optional, defaults to default_price_ct_per_kwh).
            percentage: The percentage factor (optional, defaults to default_percentage).

        Returns:
            A list of settlement amounts in EUR.
        """
        if price_ct_per_kwh is None:
            price_ct_per_kwh = self.default_price_ct_per_kwh
        if percentage is None:
            percentage = self.default_percentage
        effective_price = price_ct_per_kwh * percentage / 100
        return [(deviation * effective_price) / 100 for deviation in deviations]

    def generate_summary(self, deviations: List[float], price_ct_per_kwh: int = None, percentage: int = None) -> Dict[str, float]:
        """
        Generates a summary of settlements for a list of deviations.

        Args:
            deviations: A list of deviations in kWh.
            price_ct_per_kwh: The price in cents per kWh (optional, defaults to default_price_ct_per_kwh).
            percentage: The percentage factor (optional, defaults to default_percentage).

        Returns:
            A dictionary with total deviation, total settlement, and additional metrics.
        """
        if price_ct_per_kwh is None:
            price_ct_per_kwh = self.default_price_ct_per_kwh
        if percentage is None:
            percentage = self.default_percentage
        effective_price = price_ct_per_kwh * percentage / 100
        total_deviation = sum(deviations)
        total_settlement = sum((dev * effective_price) / 100 for dev in deviations)
        total_positive = sum(dev for dev in deviations if dev > 0)
        total_negative = sum(-dev for dev in deviations if dev < 0)
        return {
            "total_deviation_kwh": total_deviation,
            "total_settlement_eur": total_settlement,
            "total_positive_deviation_kwh": total_positive,
            "total_negative_deviation_kwh": total_negative
        }

    def calculate_portfolio_settlement(self, deviation_df) -> Dict[str, float]:
        """
        Calculates the total settlement for the portfolio.

        Args:
            deviation_df: A pandas DataFrame with a 'deviation_kwh' column.

        Returns:
            A dictionary with the total deviation and the total settlement amount in EUR.
        """
        total_deviation_kwh = deviation_df['deviation_kwh'].sum()
        total_settlement_eur = (total_deviation_kwh * self.default_price_ct_per_kwh * self.default_percentage) / 100
        
        return {
            "total_deviation_kwh": total_deviation_kwh,
            "total_settlement_eur": total_settlement_eur,
        }

    def calculate_individual_settlements(self, individual_deviation_df) -> Dict[str, Dict[str, float]]:
        """
        Calculates settlement for each individual metering point.

        Args:
            individual_deviation_df: A pandas DataFrame with 'metering_point_id' and 'deviation_kwh' columns.

        Returns:
            A nested dictionary with settlement details for each metering point.
        """
        # Group by metering point and sum deviations
        settlements = individual_deviation_df.groupby('metering_point_id')['deviation_kwh'].sum()
        
        # Calculate settlement amount for each
        settlements_eur = (settlements * self.default_price_ct_per_kwh * self.default_percentage) / 100
        
        # Format the output
        result = {
            meter_id: {
                "deviation_kwh": deviation,
                "settlement_eur": settlement_amount
            }
            for meter_id, deviation, settlement_amount in zip(settlements.index, settlements, settlements_eur)
        }
        
        return result
