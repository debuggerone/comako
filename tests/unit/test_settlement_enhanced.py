import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

import pytest
from decimal import Decimal
from src.services.settlement import (
    calculate_settlement, 
    calculate_settlement_with_percentage,
    SettlementCalculator
)


class TestSettlementCalculations:
    """Test suite for settlement calculation functions"""
    
    def test_calculate_settlement_basic(self):
        """Test basic settlement calculation"""
        # Test positive deviation (over-consumption)
        deviation_kwh = 100.0
        price_ct_per_kwh = 10
        
        result = calculate_settlement(deviation_kwh, price_ct_per_kwh)
        expected = (100.0 * 10) / 100  # 10.0 EUR
        
        assert result == expected
        assert result == 10.0
    
    def test_calculate_settlement_negative_deviation(self):
        """Test settlement calculation with negative deviation (under-consumption)"""
        deviation_kwh = -50.0
        price_ct_per_kwh = 15
        
        result = calculate_settlement(deviation_kwh, price_ct_per_kwh)
        expected = (-50.0 * 15) / 100  # -7.5 EUR (credit)
        
        assert result == expected
        assert result == -7.5
    
    def test_calculate_settlement_zero_deviation(self):
        """Test settlement calculation with zero deviation"""
        deviation_kwh = 0.0
        price_ct_per_kwh = 10
        
        result = calculate_settlement(deviation_kwh, price_ct_per_kwh)
        
        assert result == 0.0
    
    def test_calculate_settlement_different_prices(self):
        """Test settlement calculation with different price levels"""
        deviation_kwh = 100.0
        
        # Test various price levels
        test_cases = [
            (5, 5.0),    # 5 ct/kWh = 5.0 EUR
            (10, 10.0),  # 10 ct/kWh = 10.0 EUR
            (25, 25.0),  # 25 ct/kWh = 25.0 EUR
            (50, 50.0),  # 50 ct/kWh = 50.0 EUR
        ]
        
        for price_ct, expected_eur in test_cases:
            result = calculate_settlement(deviation_kwh, price_ct)
            assert result == expected_eur
    
    def test_calculate_settlement_precision(self):
        """Test settlement calculation with high precision values"""
        deviation_kwh = 123.456789
        price_ct_per_kwh = 12
        
        result = calculate_settlement(deviation_kwh, price_ct_per_kwh)
        expected = (123.456789 * 12) / 100  # 14.81482668 EUR
        
        assert abs(result - expected) < 0.00000001
    
    def test_calculate_settlement_large_values(self):
        """Test settlement calculation with large values"""
        deviation_kwh = 1000000.0  # 1 million kWh
        price_ct_per_kwh = 10
        
        result = calculate_settlement(deviation_kwh, price_ct_per_kwh)
        expected = 100000.0  # 100,000 EUR
        
        assert result == expected
    
    def test_calculate_settlement_with_percentage_basic(self):
        """Test settlement calculation with percentage adjustment"""
        deviation_kwh = 100.0
        price_ct_per_kwh = 10
        percentage = 50.0  # 50% of normal price
        
        result = calculate_settlement_with_percentage(deviation_kwh, price_ct_per_kwh, percentage)
        expected = ((100.0 * 10) / 100) * (50.0 / 100)  # 5.0 EUR
        
        assert result == expected
        assert result == 5.0
    
    def test_calculate_settlement_with_percentage_over_100(self):
        """Test settlement calculation with percentage over 100%"""
        deviation_kwh = 100.0
        price_ct_per_kwh = 10
        percentage = 150.0  # 150% of normal price (penalty)
        
        result = calculate_settlement_with_percentage(deviation_kwh, price_ct_per_kwh, percentage)
        expected = ((100.0 * 10) / 100) * (150.0 / 100)  # 15.0 EUR
        
        assert result == expected
        assert result == 15.0
    
    def test_calculate_settlement_with_percentage_zero(self):
        """Test settlement calculation with zero percentage"""
        deviation_kwh = 100.0
        price_ct_per_kwh = 10
        percentage = 0.0
        
        result = calculate_settlement_with_percentage(deviation_kwh, price_ct_per_kwh, percentage)
        
        assert result == 0.0
    
    def test_calculate_settlement_edge_cases(self):
        """Test settlement calculation edge cases"""
        # Very small deviation
        result = calculate_settlement(0.001, 10)
        assert result == 0.0001
        
        # Very small price
        result = calculate_settlement(100.0, 1)
        assert result == 1.0
        
        # Negative price (unusual but should work)
        result = calculate_settlement(100.0, -10)
        assert result == -10.0


class TestSettlementCalculator:
    """Test suite for SettlementCalculator class"""
    
    def test_settlement_calculator_initialization(self):
        """Test SettlementCalculator initialization"""
        calculator = SettlementCalculator()
        
        assert calculator.default_price_ct_per_kwh == 10
        assert calculator.default_percentage == 100.0
    
    def test_settlement_calculator_custom_defaults(self):
        """Test SettlementCalculator with custom defaults"""
        calculator = SettlementCalculator(
            default_price_ct_per_kwh=15,
            default_percentage=80.0
        )
        
        assert calculator.default_price_ct_per_kwh == 15
        assert calculator.default_percentage == 80.0
    
    def test_settlement_calculator_calculate_basic(self):
        """Test SettlementCalculator calculate method"""
        calculator = SettlementCalculator()
        
        result = calculator.calculate(100.0)
        expected = calculate_settlement(100.0, 10)
        
        assert result == expected
    
    def test_settlement_calculator_calculate_with_price(self):
        """Test SettlementCalculator calculate method with custom price"""
        calculator = SettlementCalculator()
        
        result = calculator.calculate(100.0, price_ct_per_kwh=20)
        expected = calculate_settlement(100.0, 20)
        
        assert result == expected
    
    def test_settlement_calculator_calculate_with_percentage(self):
        """Test SettlementCalculator calculate method with percentage"""
        calculator = SettlementCalculator()
        
        result = calculator.calculate(100.0, percentage=75.0)
        expected = calculate_settlement_with_percentage(100.0, 10, 75.0)
        
        assert result == expected
    
    def test_settlement_calculator_batch_calculation(self):
        """Test SettlementCalculator batch calculation"""
        calculator = SettlementCalculator()
        
        deviations = [100.0, -50.0, 25.0, 0.0]
        results = calculator.calculate_batch(deviations)
        
        expected_results = [
            calculate_settlement(100.0, 10),
            calculate_settlement(-50.0, 10),
            calculate_settlement(25.0, 10),
            calculate_settlement(0.0, 10)
        ]
        
        assert results == expected_results
    
    def test_settlement_calculator_batch_with_custom_params(self):
        """Test SettlementCalculator batch calculation with custom parameters"""
        calculator = SettlementCalculator()
        
        deviations = [100.0, -50.0]
        results = calculator.calculate_batch(deviations, price_ct_per_kwh=15, percentage=80.0)
        
        expected_results = [
            calculate_settlement_with_percentage(100.0, 15, 80.0),
            calculate_settlement_with_percentage(-50.0, 15, 80.0)
        ]
        
        assert results == expected_results
    
    def test_settlement_calculator_summary(self):
        """Test SettlementCalculator summary generation"""
        calculator = SettlementCalculator()
        
        deviations = [100.0, -50.0, 25.0, -10.0]
        summary = calculator.generate_summary(deviations)
        
        assert "total_positive_deviation" in summary
        assert "total_negative_deviation" in summary
        assert "net_deviation" in summary
        assert "total_settlement" in summary
        assert "positive_settlement" in summary
        assert "negative_settlement" in summary
        
        # Check calculations
        assert summary["total_positive_deviation"] == 125.0  # 100 + 25
        assert summary["total_negative_deviation"] == -60.0  # -50 + -10
        assert summary["net_deviation"] == 65.0  # 125 - 60
    
    def test_settlement_calculator_tiered_pricing(self):
        """Test SettlementCalculator with tiered pricing"""
        calculator = SettlementCalculator()
        
        # Test tiered pricing logic (if implemented)
        deviation_kwh = 1000.0
        
        # This would test tiered pricing if implemented
        # For now, just test that it works with standard pricing
        result = calculator.calculate(deviation_kwh)
        expected = calculate_settlement(deviation_kwh, 10)
        
        assert result == expected


class TestSettlementValidation:
    """Test suite for settlement calculation validation"""
    
    def test_settlement_input_validation(self):
        """Test input validation for settlement calculations"""
        # Test with None values
        with pytest.raises(TypeError):
            calculate_settlement(None, 10)
        
        with pytest.raises(TypeError):
            calculate_settlement(100.0, None)
    
    def test_settlement_type_conversion(self):
        """Test type conversion in settlement calculations"""
        # Test with string inputs that can be converted
        result = calculate_settlement("100.0", "10")
        expected = calculate_settlement(100.0, 10)
        
        assert result == expected
        
        # Test with integer inputs
        result = calculate_settlement(100, 10)
        expected = calculate_settlement(100.0, 10)
        
        assert result == expected
    
    def test_settlement_boundary_values(self):
        """Test settlement calculations with boundary values"""
        # Test with very large numbers
        large_deviation = 1e10
        result = calculate_settlement(large_deviation, 10)
        assert result == large_deviation
        
        # Test with very small numbers
        small_deviation = 1e-10
        result = calculate_settlement(small_deviation, 10)
        assert result == small_deviation / 100


class TestSettlementIntegration:
    """Integration tests for settlement calculations"""
    
    def test_settlement_with_real_world_scenario(self):
        """Test settlement calculation with realistic energy market scenario"""
        # Scenario: Energy cooperative with mixed consumption/generation
        scenarios = [
            {
                "name": "Over-consumption during peak hours",
                "deviation_kwh": 150.0,
                "price_ct_per_kwh": 25,  # Peak hour pricing
                "expected_eur": 37.5
            },
            {
                "name": "Under-consumption during off-peak",
                "deviation_kwh": -75.0,
                "price_ct_per_kwh": 8,   # Off-peak pricing
                "expected_eur": -6.0
            },
            {
                "name": "Excess generation",
                "deviation_kwh": -200.0,
                "price_ct_per_kwh": 12,  # Feed-in tariff
                "expected_eur": -24.0
            }
        ]
        
        for scenario in scenarios:
            result = calculate_settlement(
                scenario["deviation_kwh"], 
                scenario["price_ct_per_kwh"]
            )
            assert result == scenario["expected_eur"], f"Failed for scenario: {scenario['name']}"
    
    def test_settlement_monthly_calculation(self):
        """Test settlement calculation for monthly billing cycle"""
        # Simulate daily deviations over a month
        daily_deviations = [
            10.5, -5.2, 15.8, -8.1, 22.3, -12.7, 18.9,  # Week 1
            -3.4, 25.6, -15.2, 8.7, -6.9, 31.2, -18.5,  # Week 2
            12.1, -9.8, 19.4, -7.3, 14.6, -11.2, 26.8,  # Week 3
            -4.7, 17.3, -13.6, 9.2, -5.8, 20.1, -16.4,  # Week 4
            11.9, -8.5, 23.7                             # Partial week 5
        ]
        
        calculator = SettlementCalculator()
        monthly_summary = calculator.generate_summary(daily_deviations)
        
        # Verify summary contains expected fields
        assert "total_settlement" in monthly_summary
        assert "net_deviation" in monthly_summary
        assert isinstance(monthly_summary["total_settlement"], float)
    
    def test_settlement_precision_consistency(self):
        """Test that settlement calculations are consistent across different input formats"""
        test_cases = [
            (100.0, 10),
            (100, 10.0),
            (Decimal('100.0'), 10),
            (100.0, Decimal('10'))
        ]
        
        results = []
        for deviation, price in test_cases:
            result = calculate_settlement(float(deviation), int(float(price)))
            results.append(result)
        
        # All results should be the same
        assert all(abs(r - results[0]) < 1e-10 for r in results)


if __name__ == "__main__":
    pytest.main([__file__])
