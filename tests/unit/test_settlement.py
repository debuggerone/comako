import pytest
from src.services.settlement import calculate_settlement, calculate_settlement_with_percentage

def test_calculate_settlement():
    deviation_kwh = 100.0
    price_ct_per_kwh = 10
    settlement_eur = calculate_settlement(deviation_kwh, price_ct_per_kwh)
    assert settlement_eur == 10.0

def test_calculate_settlement_with_percentage():
    deviation_kwh = 100.0
    forecast_kwh = 1000.0
    price_ct_per_kwh = 10
    result = calculate_settlement_with_percentage(deviation_kwh, forecast_kwh, price_ct_per_kwh)
    assert result["settlement_eur"] == 10.0
    assert result["deviation_percentage"] == 10.0
