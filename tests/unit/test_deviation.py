import pytest
from src.services.deviation import calculate_deviation, calculate_deviation_percentage

def test_calculate_deviation():
    actual = 100.0
    forecast = 90.0
    deviation = calculate_deviation(actual, forecast)
    assert deviation == 10.0

def test_calculate_deviation_percentage():
    actual = 100.0
    forecast = 90.0
    deviation_percentage = calculate_deviation_percentage(actual, forecast)
    assert abs(deviation_percentage - 11.11111111111111) < 0.000001

def test_calculate_deviation_percentage_zero_forecast():
    actual = 100.0
    forecast = 0.0
    deviation_percentage = calculate_deviation_percentage(actual, forecast)
    assert deviation_percentage == float('inf')
