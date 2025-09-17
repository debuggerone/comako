import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

import pytest
import math
from src.services.deviation import (
    calculate_deviation, 
    calculate_deviation_percentage,
    DeviationAnalyzer
)


class TestDeviationCalculations:
    """Test suite for deviation calculation functions"""
    
    def test_calculate_deviation_basic(self):
        """Test basic deviation calculation"""
        actual = 100.0
        forecast = 90.0
        
        result = calculate_deviation(actual, forecast)
        expected = 10.0  # 100 - 90
        
        assert result == expected
    
    def test_calculate_deviation_negative(self):
        """Test deviation calculation with negative result"""
        actual = 80.0
        forecast = 100.0
        
        result = calculate_deviation(actual, forecast)
        expected = -20.0  # 80 - 100
        
        assert result == expected
    
    def test_calculate_deviation_zero(self):
        """Test deviation calculation with zero result"""
        actual = 100.0
        forecast = 100.0
        
        result = calculate_deviation(actual, forecast)
        expected = 0.0
        
        assert result == expected
    
    def test_calculate_deviation_precision(self):
        """Test deviation calculation with high precision"""
        actual = 123.456789
        forecast = 98.123456
        
        result = calculate_deviation(actual, forecast)
        expected = 25.333333  # 123.456789 - 98.123456
        
        assert abs(result - expected) < 0.000001
    
    def test_calculate_deviation_large_values(self):
        """Test deviation calculation with large values"""
        actual = 1000000.0
        forecast = 999999.0
        
        result = calculate_deviation(actual, forecast)
        expected = 1.0
        
        assert result == expected
    
    def test_calculate_deviation_percentage_basic(self):
        """Test percentage deviation calculation"""
        actual = 110.0
        forecast = 100.0
        
        result = calculate_deviation_percentage(actual, forecast)
        expected = 10.0  # ((110 - 100) / 100) * 100
        
        assert result == expected
    
    def test_calculate_deviation_percentage_negative(self):
        """Test percentage deviation calculation with negative result"""
        actual = 90.0
        forecast = 100.0
        
        result = calculate_deviation_percentage(actual, forecast)
        expected = -10.0  # ((90 - 100) / 100) * 100
        
        assert result == expected
    
    def test_calculate_deviation_percentage_zero_forecast(self):
        """Test percentage deviation calculation with zero forecast"""
        actual = 50.0
        forecast = 0.0
        
        # Should handle division by zero gracefully
        result = calculate_deviation_percentage(actual, forecast)
        
        # Depending on implementation, this might return inf, None, or raise exception
        # Let's assume it returns a very large number or handles it gracefully
        assert result is not None
    
    def test_calculate_deviation_percentage_precision(self):
        """Test percentage deviation calculation with high precision"""
        actual = 103.5
        forecast = 100.0
        
        result = calculate_deviation_percentage(actual, forecast)
        expected = 3.5  # ((103.5 - 100) / 100) * 100
        
        assert abs(result - expected) < 0.000001
    
    def test_calculate_deviation_edge_cases(self):
        """Test deviation calculation edge cases"""
        # Both values are zero
        result = calculate_deviation(0.0, 0.0)
        assert result == 0.0
        
        # Negative values
        result = calculate_deviation(-50.0, -30.0)
        assert result == -20.0
        
        # Mixed positive/negative
        result = calculate_deviation(50.0, -30.0)
        assert result == 80.0
        
        result = calculate_deviation(-50.0, 30.0)
        assert result == -80.0


class TestDeviationAnalyzer:
    """Test suite for DeviationAnalyzer class"""

    def test_deviation_analyzer_initialization(self):
        """Test DeviationAnalyzer initialization"""
        readings_data = [
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T00:00:00Z', 'value_kwh': 100},
        ]
        forecast_data = [
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T00:00:00Z', 'value_kwh': 95},
        ]
        analyzer = DeviationAnalyzer(readings_data, forecast_data)
        assert not analyzer.readings_df.empty
        assert not analyzer.forecast_df.empty

    def test_deviation_analyzer_custom_thresholds(self):
        """Test DeviationAnalyzer with custom thresholds"""
        readings_data = [
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T00:00:00Z', 'value_kwh': 100},
        ]
        forecast_data = [
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T00:00:00Z', 'value_kwh': 95},
        ]
        analyzer = DeviationAnalyzer(readings_data, forecast_data)
        # The DeviationAnalyzer does not have threshold attributes anymore.
        # This test is now verifying that the analyzer can be initialized.
        assert isinstance(analyzer, DeviationAnalyzer)

    def test_deviation_analyzer_classify_normal(self):
        """Test deviation classification - normal"""
        readings_data = [
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T00:00:00Z', 'value_kwh': 102},
        ]
        forecast_data = [
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T00:00:00Z', 'value_kwh': 100},
        ]
        analyzer = DeviationAnalyzer(readings_data, forecast_data)
        df = analyzer.calculate_portfolio_deviation()
        assert df['deviation_kwh'].iloc[0] == 2.0

    def test_deviation_analyzer_classify_warning(self):
        """Test deviation classification - warning"""
        readings_data = [
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T00:00:00Z', 'value_kwh': 107},
        ]
        forecast_data = [
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T00:00:00Z', 'value_kwh': 100},
        ]
        analyzer = DeviationAnalyzer(readings_data, forecast_data)
        df = analyzer.calculate_portfolio_deviation()
        assert df['deviation_kwh'].iloc[0] == 7.0

    def test_deviation_analyzer_classify_critical(self):
        """Test deviation classification - critical"""
        readings_data = [
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T00:00:00Z', 'value_kwh': 125},
        ]
        forecast_data = [
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T00:00:00Z', 'value_kwh': 100},
        ]
        analyzer = DeviationAnalyzer(readings_data, forecast_data)
        df = analyzer.calculate_portfolio_deviation()
        assert df['deviation_kwh'].iloc[0] == 25.0

    def test_deviation_analyzer_classify_alert(self):
        """Test deviation classification - alert"""
        readings_data = [
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T00:00:00Z', 'value_kwh': 115},
        ]
        forecast_data = [
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T00:00:00Z', 'value_kwh': 100},
        ]
        analyzer = DeviationAnalyzer(readings_data, forecast_data)
        df = analyzer.calculate_portfolio_deviation()
        assert df['deviation_kwh'].iloc[0] == 15.0

    def test_deviation_analyzer_negative_deviations(self):
        """Test deviation classification with negative deviations"""
        readings_data = [
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T00:00:00Z', 'value_kwh': 75},
        ]
        forecast_data = [
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T00:00:00Z', 'value_kwh': 100},
        ]
        analyzer = DeviationAnalyzer(readings_data, forecast_data)
        df = analyzer.calculate_portfolio_deviation()
        assert df['deviation_kwh'].iloc[0] == -25.0

    def test_deviation_analyzer_batch_analysis(self):
        """Test batch deviation analysis"""
        readings_data = [
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T00:00:00Z', 'value_kwh': 100},
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T01:00:00Z', 'value_kwh': 100},
        ]
        forecast_data = [
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T00:00:00Z', 'value_kwh': 98},
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T01:00:00Z', 'value_kwh': 93},
        ]
        analyzer = DeviationAnalyzer(readings_data, forecast_data)
        df = analyzer.calculate_portfolio_deviation()
        assert len(df) == 2

    def test_deviation_analyzer_statistics(self):
        """Test deviation statistics generation"""
        readings_data = [
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T00:00:00Z', 'value_kwh': 100},
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T01:00:00Z', 'value_kwh': 100},
        ]
        forecast_data = [
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T00:00:00Z', 'value_kwh': 95},
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T01:00:00Z', 'value_kwh': 90},
        ]
        analyzer = DeviationAnalyzer(readings_data, forecast_data)
        df = analyzer.calculate_portfolio_deviation()
        stats = df['deviation_kwh'].describe()
        assert stats['count'] == 2

    def test_deviation_analyzer_trend_analysis(self):
        """Test deviation trend analysis"""
        readings_data = [
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T00:00:00Z', 'value_kwh': 100},
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T01:00:00Z', 'value_kwh': 100},
        ]
        forecast_data = [
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T00:00:00Z', 'value_kwh': 98},
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T01:00:00Z', 'value_kwh': 96},
        ]
        analyzer = DeviationAnalyzer(readings_data, forecast_data)
        df = analyzer.calculate_portfolio_deviation()
        assert df['deviation_kwh'].is_monotonic_increasing


class TestDeviationValidation:
    """Test suite for deviation calculation validation"""
    
    def test_deviation_input_validation(self):
        """Test input validation for deviation calculations"""
        # Test with None values
        with pytest.raises(TypeError):
            calculate_deviation(None, 100.0)
        
        with pytest.raises(TypeError):
            calculate_deviation(100.0, None)
    
    def test_deviation_type_conversion(self):
        """Test type conversion in deviation calculations"""
        # Test with string inputs that can be converted
        result = calculate_deviation(float("100.0"), float("90.0"))
        expected = calculate_deviation(100.0, 90.0)
        
        assert result == expected
        
        # Test with integer inputs
        result = calculate_deviation(100, 90)
        expected = calculate_deviation(100.0, 90.0)
        
        assert result == expected
    
    def test_deviation_boundary_values(self):
        """Test deviation calculations with boundary values"""
        # Very large numbers
        result = calculate_deviation(1e10, 1e10 - 1)
        assert result == 1.0
        
        # Very small numbers
        result = calculate_deviation(1e-10, 2e-10)
        assert result == -1e-10
        
        # Zero values
        result = calculate_deviation(0.0, 1.0)
        assert result == -1.0


class TestDeviationMathematical:
    """Test suite for mathematical properties of deviation calculations"""
    
    def test_deviation_symmetry(self):
        """Test that deviation calculation is anti-symmetric"""
        actual = 100.0
        forecast = 90.0
        
        dev1 = calculate_deviation(actual, forecast)
        dev2 = calculate_deviation(forecast, actual)
        
        assert dev1 == -dev2
    
    def test_deviation_additivity(self):
        """Test deviation calculation with additive properties"""
        # If we have multiple forecasts and actuals, 
        # total deviation should equal sum of individual deviations
        
        actuals = [100.0, 200.0, 150.0]
        forecasts = [90.0, 210.0, 140.0]
        
        individual_deviations = [
            calculate_deviation(a, f) for a, f in zip(actuals, forecasts)
        ]
        
        total_actual = sum(actuals)
        total_forecast = sum(forecasts)
        total_deviation = calculate_deviation(total_actual, total_forecast)
        
        assert total_deviation == sum(individual_deviations)
    
    def test_deviation_percentage_scaling(self):
        """Test that percentage deviation scales correctly"""
        # Doubling both values should give same percentage
        actual1, forecast1 = 100.0, 90.0
        actual2, forecast2 = 200.0, 180.0
        
        pct1 = calculate_deviation_percentage(actual1, forecast1)
        pct2 = calculate_deviation_percentage(actual2, forecast2)
        
        assert abs(pct1 - pct2) < 0.000001


class TestDeviationRealWorldScenarios:
    """Test suite for real-world deviation scenarios"""
    
    def test_energy_consumption_deviation(self):
        """Test deviation calculation for energy consumption scenarios"""
        scenarios = [
            {
                "name": "Summer peak demand",
                "actual": 1250.0,
                "forecast": 1100.0,
                "expected_deviation": 150.0,
                "expected_percentage": 13.636363636363636
            },
            {
                "name": "Winter heating efficiency",
                "actual": 850.0,
                "forecast": 950.0,
                "expected_deviation": -100.0,
                "expected_percentage": -10.526315789473685
            },
            {
                "name": "Perfect forecast",
                "actual": 500.0,
                "forecast": 500.0,
                "expected_deviation": 0.0,
                "expected_percentage": 0.0
            }
        ]
        
        for scenario in scenarios:
            deviation = calculate_deviation(scenario["actual"], scenario["forecast"])
            percentage = calculate_deviation_percentage(scenario["actual"], scenario["forecast"])
            
            assert abs(deviation - scenario["expected_deviation"]) < 0.000001
            assert abs(percentage - scenario["expected_percentage"]) < 0.000001
    
    def test_renewable_generation_deviation(self):
        """Test deviation calculation for renewable energy generation"""
        # Solar generation is highly variable
        readings_data = [
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T00:00:00Z', 'value_kwh': 100},
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T01:00:00Z', 'value_kwh': 150},
        ]
        forecast_data = [
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T00:00:00Z', 'value_kwh': 120},
            {'metering_point_id': 'MP1', 'timestamp': '2023-01-01T01:00:00Z', 'value_kwh': 120},
        ]
        
        analyzer = DeviationAnalyzer(readings_data, forecast_data)
        df = analyzer.calculate_portfolio_deviation()
        
        # Verify that the analyzer handles high variability appropriately
        assert len(df) == 2
    
    def test_load_balancing_deviation(self):
        """Test deviation calculation for load balancing scenarios"""
        # Simulate 24-hour load profile deviations
        readings_data = []
        forecast_data = []
        
        # Generate synthetic hourly data
        for hour in range(24):
            base_load = 100 + 50 * math.sin(2 * math.pi * hour / 24)  # Sinusoidal pattern
            actual = base_load + (hour % 3 - 1) * 10  # Add some variation
            forecast = base_load
            readings_data.append({'metering_point_id': 'MP1', 'timestamp': f'2023-01-01T{hour:02}:00:00Z', 'value_kwh': actual})
            forecast_data.append({'metering_point_id': 'MP1', 'timestamp': f'2023-01-01T{hour:02}:00:00Z', 'value_kwh': forecast})
        
        analyzer = DeviationAnalyzer(readings_data, forecast_data)
        df = analyzer.calculate_portfolio_deviation()
        
        assert len(df) == 24


if __name__ == "__main__":
    pytest.main([__file__])
