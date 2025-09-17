import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta
from tests.unit.test_config import TestAsyncSession, Base
from src.models.models import EnergyReading, MeteringPoint, MarketParticipant
from src.services.anomaly_detection import AnomalyDetector


@pytest_asyncio.fixture(scope="function")
async def setup_database():
    """Setup test database"""
    engine = TestAsyncSession.kw["bind"]
    await engine.dispose()
    async with engine.connect() as conn:
        if conn.in_transaction():
            await conn.rollback()
        await conn.execution_options(isolation_level="AUTOCOMMIT")
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)


@pytest_asyncio.fixture
async def sample_data(setup_database):
    """Create sample test data"""
    async with TestAsyncSession() as session:
        # Create participant
        participant = MarketParticipant(id="MP1", name="Test Participant")
        session.add(participant)
        
        # Create metering point
        metering_point = MeteringPoint(
            id="MTR1",
            market_participant_id="MP1",
            location="Test Location"
        )
        session.add(metering_point)
        
        await session.commit()
        return {
            "participant_id": "MP1",
            "metering_point_id": "MTR1"
        }


class TestAnomalyDetector:
    """Test suite for AnomalyDetector class"""
    
    def test_anomaly_detector_initialization(self):
        """Test AnomalyDetector initialization"""
        mock_session = AsyncMock()
        detector = AnomalyDetector(mock_session)
        
        assert detector.session == mock_session
    
    def test_detect_anomalies_basic(self):
        """Test basic anomaly detection"""
        mock_session = AsyncMock()
        detector = AnomalyDetector(mock_session)
        
        # Create test readings with clear outliers
        readings = [
            {"value_kwh": 100.0, "id": "1"},
            {"value_kwh": 105.0, "id": "2"},
            {"value_kwh": 110.0, "id": "3"},
            {"value_kwh": 95.0, "id": "4"},
            {"value_kwh": 500.0, "id": "5"},  # Clear outlier (high)
            {"value_kwh": 10.0, "id": "6"},   # Clear outlier (low)
        ]
        
        anomalies = detector.detect_anomalies(readings)
        
        # Should detect the outliers
        assert len(anomalies) >= 1
        anomaly_values = [a["value_kwh"] for a in anomalies]
        assert 500.0 in anomaly_values or 10.0 in anomaly_values
    
    def test_detect_anomalies_no_outliers(self):
        """Test anomaly detection with no outliers"""
        mock_session = AsyncMock()
        detector = AnomalyDetector(mock_session)
        
        # Create readings with similar values (no outliers)
        readings = [
            {"value_kwh": 100.0, "id": "1"},
            {"value_kwh": 102.0, "id": "2"},
            {"value_kwh": 98.0, "id": "3"},
            {"value_kwh": 101.0, "id": "4"},
            {"value_kwh": 99.0, "id": "5"},
        ]
        
        anomalies = detector.detect_anomalies(readings)
        
        # Should detect no anomalies
        assert len(anomalies) == 0
    
    def test_detect_anomalies_insufficient_data(self):
        """Test anomaly detection with insufficient data"""
        mock_session = AsyncMock()
        detector = AnomalyDetector(mock_session)
        
        # Less than 3 readings
        readings = [
            {"value_kwh": 100.0, "id": "1"},
            {"value_kwh": 200.0, "id": "2"},
        ]
        
        anomalies = detector.detect_anomalies(readings)
        
        # Should return empty list due to insufficient data
        assert len(anomalies) == 0
    
    def test_detect_anomalies_custom_threshold(self):
        """Test anomaly detection with custom threshold"""
        mock_session = AsyncMock()
        detector = AnomalyDetector(mock_session)
        
        readings = [
            {"value_kwh": 100.0, "id": "1"},
            {"value_kwh": 105.0, "id": "2"},
            {"value_kwh": 110.0, "id": "3"},
            {"value_kwh": 95.0, "id": "4"},
            {"value_kwh": 130.0, "id": "5"},  # Moderate outlier
        ]
        
        # Test with strict threshold (1.0 standard deviations)
        anomalies_strict = detector.detect_anomalies(readings, threshold_multiplier=1.0)
        
        # Test with lenient threshold (3.0 standard deviations)
        anomalies_lenient = detector.detect_anomalies(readings, threshold_multiplier=3.0)
        
        # Strict threshold should detect more anomalies
        assert len(anomalies_strict) >= len(anomalies_lenient)
    
    def test_detect_anomalies_metadata(self):
        """Test that anomaly detection adds proper metadata"""
        mock_session = AsyncMock()
        detector = AnomalyDetector(mock_session)
        
        readings = [
            {"value_kwh": 100.0, "id": "1"},
            {"value_kwh": 105.0, "id": "2"},
            {"value_kwh": 110.0, "id": "3"},
            {"value_kwh": 500.0, "id": "4"},  # High outlier
        ]
        
        anomalies = detector.detect_anomalies(readings)
        
        if anomalies:
            anomaly = anomalies[0]
            assert "anomaly_type" in anomaly
            assert "deviation_from_mean" in anomaly
            assert "threshold_exceeded" in anomaly
            assert anomaly["anomaly_type"] in ["high", "low"]
    
    @pytest.mark.asyncio
    async def test_get_recent_anomalies_basic(self, sample_data):
        """Test getting recent anomalies from database"""
        async with TestAsyncSession() as session:
            # Create test readings with outliers
            base_time = datetime.utcnow() - timedelta(hours=12)
            
            readings = [
                EnergyReading(
                    id="R1",
                    metering_point_id=sample_data["metering_point_id"],
                    timestamp=base_time,
                    value_kwh=100.0,
                    reading_type="consumption"
                ),
                EnergyReading(
                    id="R2",
                    metering_point_id=sample_data["metering_point_id"],
                    timestamp=base_time + timedelta(hours=1),
                    value_kwh=105.0,
                    reading_type="consumption"
                ),
                EnergyReading(
                    id="R3",
                    metering_point_id=sample_data["metering_point_id"],
                    timestamp=base_time + timedelta(hours=2),
                    value_kwh=500.0,  # Outlier
                    reading_type="consumption"
                ),
            ]
            
            session.add_all(readings)
            await session.commit()
            
            detector = AnomalyDetector(session)
            anomalies = await detector.get_recent_anomalies(days=1)
            
            # Should detect the outlier
            assert len(anomalies) >= 1
            anomaly_values = [a["value_kwh"] for a in anomalies]
            assert 500.0 in anomaly_values
    
    @pytest.mark.asyncio
    async def test_get_recent_anomalies_time_filter(self, sample_data):
        """Test time filtering in recent anomalies"""
        async with TestAsyncSession() as session:
            base_time = datetime.utcnow()
            
            # Create readings: some recent, some old
            readings = [
                # Recent reading (within 1 day)
                EnergyReading(
                    id="R1",
                    metering_point_id=sample_data["metering_point_id"],
                    timestamp=base_time - timedelta(hours=12),
                    value_kwh=500.0,  # Outlier
                    reading_type="consumption"
                ),
                # Old reading (beyond 1 day)
                EnergyReading(
                    id="R2",
                    metering_point_id=sample_data["metering_point_id"],
                    timestamp=base_time - timedelta(days=2),
                    value_kwh=600.0,  # Outlier
                    reading_type="consumption"
                ),
                # Normal recent readings
                EnergyReading(
                    id="R3",
                    metering_point_id=sample_data["metering_point_id"],
                    timestamp=base_time - timedelta(hours=6),
                    value_kwh=100.0,
                    reading_type="consumption"
                ),
                EnergyReading(
                    id="R4",
                    metering_point_id=sample_data["metering_point_id"],
                    timestamp=base_time - timedelta(hours=3),
                    value_kwh=105.0,
                    reading_type="consumption"
                ),
            ]
            
            session.add_all(readings)
            await session.commit()
            
            detector = AnomalyDetector(session)
            
            # Get anomalies from last 1 day
            recent_anomalies = await detector.get_recent_anomalies(days=1)
            
            # Should only include recent readings
            recent_timestamps = [datetime.fromisoformat(a["timestamp"]) for a in recent_anomalies]
            cutoff_time = base_time - timedelta(days=1)
            
            for timestamp in recent_timestamps:
                assert timestamp >= cutoff_time
    
    @pytest.mark.asyncio
    async def test_get_recent_anomalies_metering_point_filter(self, sample_data):
        """Test metering point filtering in recent anomalies"""
        async with TestAsyncSession() as session:
            # Create second metering point
            participant2 = MarketParticipant(id="MP2", name="Test Participant 2", address="Test Address 2")
            session.add(participant2)
            
            metering_point2 = MeteringPoint(
                id="MTR2",
                market_participant_id="MP2",
                location="Test Location 2"
            )
            session.add(metering_point2)
            
            base_time = datetime.utcnow() - timedelta(hours=12)
            
            readings = [
                # Readings for first metering point
                EnergyReading(
                    id="R1",
                    metering_point_id=sample_data["metering_point_id"],
                    timestamp=base_time,
                    value_kwh=100.0,
                    reading_type="consumption"
                ),
                EnergyReading(
                    id="R2",
                    metering_point_id=sample_data["metering_point_id"],
                    timestamp=base_time + timedelta(hours=1),
                    value_kwh=500.0,  # Outlier
                    reading_type="consumption"
                ),
                # Readings for second metering point
                EnergyReading(
                    id="R3",
                    metering_point_id="MTR2",
                    timestamp=base_time,
                    value_kwh=200.0,
                    reading_type="consumption"
                ),
                EnergyReading(
                    id="R4",
                    metering_point_id="MTR2",
                    timestamp=base_time + timedelta(hours=1),
                    value_kwh=600.0,  # Outlier
                    reading_type="consumption"
                ),
            ]
            
            session.add_all(readings)
            await session.commit()
            
            detector = AnomalyDetector(session)
            
            # Get anomalies for specific metering point
            filtered_anomalies = await detector.get_recent_anomalies(
                days=1, 
                metering_point_id=sample_data["metering_point_id"]
            )
            
            # Should only include readings from specified metering point
            for anomaly in filtered_anomalies:
                assert anomaly["metering_point_id"] == sample_data["metering_point_id"]
    
    def test_is_outlier_basic(self):
        """Test is_outlier method"""
        mock_session = AsyncMock()
        detector = AnomalyDetector(mock_session)
        
        # Test reading
        test_reading = {"value_kwh": 500.0}
        
        # Reference readings (normal values)
        reference_readings = [
            {"value_kwh": 100.0},
            {"value_kwh": 105.0},
            {"value_kwh": 110.0},
            {"value_kwh": 95.0},
            {"value_kwh": 102.0},
        ]
        
        result = detector.is_outlier(test_reading, reference_readings)
        
        # 500.0 should be detected as outlier compared to ~100 values
        assert result == True
    
    def test_is_outlier_not_outlier(self):
        """Test is_outlier method with non-outlier"""
        mock_session = AsyncMock()
        detector = AnomalyDetector(mock_session)
        
        # Test reading (within normal range)
        test_reading = {"value_kwh": 103.0}
        
        # Reference readings
        reference_readings = [
            {"value_kwh": 100.0},
            {"value_kwh": 105.0},
            {"value_kwh": 110.0},
            {"value_kwh": 95.0},
            {"value_kwh": 102.0},
        ]
        
        result = detector.is_outlier(test_reading, reference_readings)
        
        # 103.0 should not be detected as outlier
        assert result == False
    
    def test_is_outlier_insufficient_reference_data(self):
        """Test is_outlier with insufficient reference data"""
        mock_session = AsyncMock()
        detector = AnomalyDetector(mock_session)
        
        test_reading = {"value_kwh": 500.0}
        
        # Only one reference reading (insufficient)
        reference_readings = [{"value_kwh": 100.0}]
        
        result = detector.is_outlier(test_reading, reference_readings)
        
        # Should return False due to insufficient data
        assert result == False


class TestAnomalyDetectionStatistics:
    """Test suite for anomaly detection statistics and analysis"""
    
    def test_statistical_measures(self):
        """Test statistical measures used in anomaly detection"""
        mock_session = AsyncMock()
        detector = AnomalyDetector(mock_session)
        
        # Test with known statistical properties
        readings = [
            {"value_kwh": 100.0},
            {"value_kwh": 200.0},
            {"value_kwh": 300.0},
            {"value_kwh": 400.0},
            {"value_kwh": 500.0},
        ]
        
        # Mean should be 300.0
        # Standard deviation should be ~158.11
        # Values outside mean ± 2*std should be anomalies
        
        anomalies = detector.detect_anomalies(readings, threshold_multiplier=1.0)
        
        # With threshold_multiplier=1.0, more values should be flagged
        # This tests the statistical calculation indirectly
        assert isinstance(anomalies, list)
    
    def test_anomaly_detection_edge_cases(self):
        """Test anomaly detection edge cases"""
        mock_session = AsyncMock()
        detector = AnomalyDetector(mock_session)
        
        # All identical values
        identical_readings = [
            {"value_kwh": 100.0, "id": str(i)} for i in range(5)
        ]
        
        anomalies = detector.detect_anomalies(identical_readings)
        # Should detect no anomalies (std dev = 0)
        assert len(anomalies) == 0
        
        # Empty readings list
        empty_anomalies = detector.detect_anomalies([])
        assert len(empty_anomalies) == 0


class TestAnomalyDetectionIntegration:
    """Integration tests for anomaly detection"""
    
    @pytest.mark.asyncio
    async def test_anomaly_detection_real_world_scenario(self, sample_data):
        """Test anomaly detection with realistic energy consumption data"""
        async with TestAsyncSession() as session:
            base_time = datetime.utcnow() - timedelta(days=1)
            
            # Simulate realistic daily energy consumption pattern
            # Normal consumption: 80-120 kWh per hour
            # With some anomalies: equipment failure (0 kWh), peak demand (300+ kWh)
            
            readings = []
            for hour in range(24):
                # Normal consumption with some variation
                if hour in [2, 3, 4]:  # Night hours - lower consumption
                    base_consumption = 60
                elif hour in [18, 19, 20]:  # Evening peak
                    base_consumption = 140
                else:
                    base_consumption = 100
                
                # Add some random variation
                variation = (hour % 7 - 3) * 5  # -15 to +20 kWh variation
                normal_value = base_consumption + variation
                
                # Add anomalies at specific hours
                if hour == 10:  # Equipment failure
                    value = 0.0
                elif hour == 15:  # Unusual peak demand
                    value = 350.0
                else:
                    value = normal_value
                
                reading = EnergyReading(
                    id=f"R{hour}",
                    metering_point_id=sample_data["metering_point_id"],
                    timestamp=base_time + timedelta(hours=hour),
                    value_kwh=value,
                    reading_type="consumption"
                )
                readings.append(reading)
            
            session.add_all(readings)
            await session.commit()
            
            detector = AnomalyDetector(session)
            anomalies = await detector.get_recent_anomalies(days=2)
            
            # Should detect the equipment failure (0 kWh) and peak demand (350 kWh)
            assert len(anomalies) >= 1
            
            anomaly_values = [a["value_kwh"] for a in anomalies]
            # At least one of the anomalies should be detected
            assert 0.0 in anomaly_values or 350.0 in anomaly_values
    
    @pytest.mark.asyncio
    async def test_anomaly_detection_performance(self, sample_data):
        """Test anomaly detection performance with larger dataset"""
        async with TestAsyncSession() as session:
            base_time = datetime.utcnow() - timedelta(days=7)
            
            # Create a week's worth of hourly readings (168 readings)
            readings = []
            for hour in range(168):  # 7 days * 24 hours
                # Most readings are normal (90-110 kWh)
                if hour % 50 == 0:  # Occasional anomalies
                    value = 500.0  # High anomaly
                elif hour % 73 == 0:  # Different pattern for low anomalies
                    value = 10.0   # Low anomaly
                else:
                    value = 100.0 + (hour % 20 - 10)  # Normal variation
                
                reading = EnergyReading(
                    id=f"R{hour}",
                    metering_point_id=sample_data["metering_point_id"],
                    timestamp=base_time + timedelta(hours=hour),
                    value_kwh=value,
                    reading_type="consumption"
                )
                readings.append(reading)
            
            session.add_all(readings)
            await session.commit()
            
            detector = AnomalyDetector(session)
            
            # Measure performance (should complete reasonably quickly)
            import time
            start_time = time.time()
            
            anomalies = await detector.get_recent_anomalies(days=7)
            
            end_time = time.time()
            processing_time = end_time - start_time
            
            # Should complete within reasonable time (< 5 seconds for 168 readings)
            assert processing_time < 5.0
            
            # Should detect some anomalies
            assert len(anomalies) > 0


if __name__ == "__main__":
    pytest.main([__file__])
