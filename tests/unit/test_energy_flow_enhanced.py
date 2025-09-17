import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta
from tests.unit.test_config import TestAsyncSession, Base
from src.models.models import (
    BalanceGroup, BalanceGroupMember, MarketParticipant,
    MeteringPoint, EnergyReading
)
from src.services.energy_flow import EnergyFlowAggregator


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
        # Create balance group
        balance_group = BalanceGroup(id="BG123", name="Test Group")
        session.add(balance_group)
        
        # Create participants
        participant1 = MarketParticipant(id="MP1", name="Participant 1", address="Address 1")
        participant2 = MarketParticipant(id="MP2", name="Participant 2", address="Address 2")
        session.add_all([participant1, participant2])
        
        # Create balance group members
        member1 = BalanceGroupMember(
            balance_group_id="BG123",
            market_participant_id="MP1"
        )
        member2 = BalanceGroupMember(
            balance_group_id="BG123",
            market_participant_id="MP2"
        )
        session.add_all([member1, member2])
        
        # Create metering points
        meter1 = MeteringPoint(
            id="MTR1",
            market_participant_id="MP1",
            location="Location 1"
        )
        meter2 = MeteringPoint(
            id="MTR2",
            market_participant_id="MP2",
            location="Location 2"
        )
        session.add_all([meter1, meter2])
        
        await session.commit()
        return {
            "balance_group_id": "BG123",
            "participant_ids": ["MP1", "MP2"],
            "meter_ids": ["MTR1", "MTR2"]
        }


class TestEnergyFlowAggregator:
    """Test suite for EnergyFlowAggregator"""
    
    @pytest.mark.asyncio
    async def test_aggregate_energy_flows_basic(self, sample_data):
        """Test basic energy flow aggregation"""
        async with TestAsyncSession() as session:
            # Create energy readings
            base_time = datetime.utcnow() - timedelta(hours=2)
            readings = [
                EnergyReading(
                    id="R1",
                    metering_point_id="MTR1",
                    timestamp=base_time,
                    value_kwh=100.0,
                    reading_type="consumption"
                ),
                EnergyReading(
                    id="R2",
                    metering_point_id="MTR1",
                    timestamp=base_time + timedelta(hours=1),
                    value_kwh=150.0,
                    reading_type="consumption"
                ),
                EnergyReading(
                    id="R3",
                    metering_point_id="MTR2",
                    timestamp=base_time,
                    value_kwh=75.0,
                    reading_type="generation"
                ),
                EnergyReading(
                    id="R4",
                    metering_point_id="MTR2",
                    timestamp=base_time + timedelta(hours=1),
                    value_kwh=125.0,
                    reading_type="generation"
                )
            ]
            session.add_all(readings)
            await session.commit()
            
            # Test aggregation
            aggregator = EnergyFlowAggregator(session)
            result = await aggregator.aggregate_energy_flows("BG123")
            
            assert result["consumption_kwh"] == 250.0  # 100 + 150
            assert result["generation_kwh"] == 200.0   # 75 + 125
            assert result["net_kwh"] == -50.0          # 250 - 200 (net consumption)
    
    @pytest.mark.asyncio
    async def test_aggregate_energy_flows_with_time_range(self, sample_data):
        """Test energy flow aggregation with time range filtering"""
        async with TestAsyncSession() as session:
            base_time = datetime.utcnow() - timedelta(hours=5)
            
            # Create readings across different time periods
            readings = [
                # Within range
                EnergyReading(
                    id="R1",
                    metering_point_id="MTR1",
                    timestamp=base_time + timedelta(hours=2),
                    value_kwh=100.0,
                    reading_type="consumption"
                ),
                EnergyReading(
                    id="R2",
                    metering_point_id="MTR2",
                    timestamp=base_time + timedelta(hours=3),
                    value_kwh=50.0,
                    reading_type="generation"
                ),
                # Outside range (too early)
                EnergyReading(
                    id="R3",
                    metering_point_id="MTR1",
                    timestamp=base_time,
                    value_kwh=200.0,
                    reading_type="consumption"
                ),
                # Outside range (too late)
                EnergyReading(
                    id="R4",
                    metering_point_id="MTR2",
                    timestamp=base_time + timedelta(hours=6),
                    value_kwh=75.0,
                    reading_type="generation"
                )
            ]
            session.add_all(readings)
            await session.commit()
            
            # Test with time range
            aggregator = EnergyFlowAggregator(session)
            start_time = base_time + timedelta(hours=1)
            end_time = base_time + timedelta(hours=4)
            
            result = await aggregator.aggregate_energy_flows(
                "BG123", 
                start_time=start_time, 
                end_time=end_time
            )
            
            # Should only include readings within the time range
            assert result["consumption_kwh"] == 100.0
            assert result["generation_kwh"] == 50.0
            assert result["net_kwh"] == -50.0
    
    @pytest.mark.asyncio
    async def test_aggregate_energy_flows_empty_balance_group(self, setup_database):
        """Test aggregation for non-existent balance group"""
        async with TestAsyncSession() as session:
            aggregator = EnergyFlowAggregator(session)
            result = await aggregator.aggregate_energy_flows("NONEXISTENT")
            
            assert result["consumption_kwh"] == 0.0
            assert result["generation_kwh"] == 0.0
            assert result["net_kwh"] == 0.0
    
    @pytest.mark.asyncio
    async def test_aggregate_energy_flows_no_readings(self, sample_data):
        """Test aggregation when no readings exist"""
        async with TestAsyncSession() as session:
            aggregator = EnergyFlowAggregator(session)
            result = await aggregator.aggregate_energy_flows("BG123")
            
            assert result["consumption_kwh"] == 0.0
            assert result["generation_kwh"] == 0.0
            assert result["net_kwh"] == 0.0
    
    @pytest.mark.asyncio
    async def test_aggregate_energy_flows_mixed_reading_types(self, sample_data):
        """Test aggregation with various reading types"""
        async with TestAsyncSession() as session:
            base_time = datetime.utcnow() - timedelta(hours=1)
            
            readings = [
                EnergyReading(
                    id="R1",
                    metering_point_id="MTR1",
                    timestamp=base_time,
                    value_kwh=100.0,
                    reading_type="consumption"
                ),
                EnergyReading(
                    id="R2",
                    metering_point_id="MTR1",
                    timestamp=base_time,
                    value_kwh=50.0,
                    reading_type="generation"
                ),
                EnergyReading(
                    id="R3",
                    metering_point_id="MTR2",
                    timestamp=base_time,
                    value_kwh=75.0,
                    reading_type="net"  # Should be treated as consumption
                ),
                EnergyReading(
                    id="R4",
                    metering_point_id="MTR2",
                    timestamp=base_time,
                    value_kwh=25.0,
                    reading_type="unknown"  # Should be treated as consumption
                )
            ]
            session.add_all(readings)
            await session.commit()
            
            aggregator = EnergyFlowAggregator(session)
            result = await aggregator.aggregate_energy_flows("BG123")
            
            # consumption: 100 + 75 + 25 = 200
            # generation: 50
            assert result["consumption_kwh"] == 200.0
            assert result["generation_kwh"] == 50.0
            assert result["net_kwh"] == -150.0
    
    @pytest.mark.asyncio
    async def test_aggregate_energy_flows_large_dataset(self, sample_data):
        """Test aggregation performance with larger dataset"""
        async with TestAsyncSession() as session:
            base_time = datetime.utcnow() - timedelta(hours=24)
            
            # Create 100 readings
            readings = []
            for i in range(100):
                reading = EnergyReading(
                    id=f"R{i}",
                    metering_point_id="MTR1" if i % 2 == 0 else "MTR2",
                    timestamp=base_time + timedelta(minutes=i * 15),
                    value_kwh=10.0 + (i % 10),  # Values from 10-19
                    reading_type="consumption" if i % 3 == 0 else "generation"
                )
                readings.append(reading)
            
            session.add_all(readings)
            await session.commit()
            
            aggregator = EnergyFlowAggregator(session)
            result = await aggregator.aggregate_energy_flows("BG123")
            
            # Verify we get reasonable results
            assert result["consumption_kwh"] > 0
            assert result["generation_kwh"] > 0
            assert isinstance(result["net_kwh"], float)
    
    @pytest.mark.asyncio
    async def test_aggregate_energy_flows_precision(self, sample_data):
        """Test aggregation with decimal precision"""
        async with TestAsyncSession() as session:
            base_time = datetime.utcnow() - timedelta(hours=1)
            
            readings = [
                EnergyReading(
                    id="R1",
                    metering_point_id="MTR1",
                    timestamp=base_time,
                    value_kwh=10.123456,
                    reading_type="consumption"
                ),
                EnergyReading(
                    id="R2",
                    metering_point_id="MTR2",
                    timestamp=base_time,
                    value_kwh=20.987654,
                    reading_type="consumption"
                )
            ]
            session.add_all(readings)
            await session.commit()
            
            aggregator = EnergyFlowAggregator(session)
            result = await aggregator.aggregate_energy_flows("BG123")
            
            # Test precision is maintained
            expected_total = 10.123456 + 20.987654
            assert abs(result["consumption_kwh"] - expected_total) < 0.000001
    
    def test_energy_flow_aggregator_initialization(self):
        """Test EnergyFlowAggregator initialization"""
        mock_session = AsyncMock()
        aggregator = EnergyFlowAggregator(mock_session)
        
        assert aggregator.session == mock_session
    
    @pytest.mark.asyncio
    async def test_aggregate_energy_flows_database_error(self, sample_data):
        """Test handling of database errors during aggregation"""
        # Create a mock session that raises an exception
        mock_session = AsyncMock()
        mock_session.execute.side_effect = Exception("Database connection failed")
        
        aggregator = EnergyFlowAggregator(mock_session)
        
        # Should raise the exception
        with pytest.raises(Exception, match="Database connection failed"):
            await aggregator.aggregate_energy_flows("BG123")


# Additional utility tests
class TestEnergyFlowUtilities:
    """Test utility functions related to energy flow"""
    
    def test_reading_type_classification(self):
        """Test reading type classification logic"""
        # This would test any utility functions for classifying reading types
        # Since the actual implementation might vary, this is a placeholder
        pass
    
    def test_energy_flow_calculations(self):
        """Test energy flow calculation utilities"""
        # Test any calculation utilities
        pass


if __name__ == "__main__":
    pytest.main([__file__])
