import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

import pytest
import pytest_asyncio
from tests.unit.test_config import TestAsyncSession, Base
from src.models.models import (
    BalanceGroup, BalanceGroupMember, MarketParticipant,
    MeteringPoint, EnergyReading
)
from src.services.energy_flow import aggregate_energy_flows

@pytest_asyncio.fixture(scope="function")
async def setup_database():
    engine = TestAsyncSession.kw["bind"]
    await engine.dispose()
    async with engine.connect() as conn:
        if conn.in_transaction():
            await conn.rollback()
        await conn.execution_options(isolation_level="AUTOCOMMIT")
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

@pytest.mark.asyncio
async def test_aggregate_energy_flows(setup_database):
    async with TestAsyncSession() as session:
        # Create balance group and members
        balance_group = BalanceGroup(id="BG123", name="Test Group")
        session.add(balance_group)
        
        participant1 = MarketParticipant(id="MP1", name="Participant 1")
        participant2 = MarketParticipant(id="MP2", name="Participant 2")
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
            type="consumption"
        )
        meter2 = MeteringPoint(
            id="MTR2",
            market_participant_id="MP2",
            type="generation"
        )
        session.add_all([meter1, meter2])
        
        # Create energy readings
        readings = [
            EnergyReading(
                metering_point_id="MTR1",
                timestamp="2025-08-03T00:00:00",
                value_kwh=10.5,
                direction="consumption"
            ),
            EnergyReading(
                metering_point_id="MTR1",
                timestamp="2025-08-03T01:00:00",
                value_kwh=15.2,
                direction="consumption"
            ),
            EnergyReading(
                metering_point_id="MTR2",
                timestamp="2025-08-03T00:00:00",
                value_kwh=20.0,
                direction="generation"
            ),
            EnergyReading(
                metering_point_id="MTR2",
                timestamp="2025-08-03T01:00:00",
                value_kwh=25.3,
                direction="generation"
            )
        ]
        session.add_all(readings)
        await session.commit()
        
        # Aggregate flows
        result = await aggregate_energy_flows("BG123", session)
        
        # Verify results
        assert result["total_consumption"] == 25.7  # 10.5 + 15.2
        assert result["total_generation"] == 45.3  # 20.0 + 25.3
        assert result["net_flow"] == 19.6  # 45.3 - 25.7
