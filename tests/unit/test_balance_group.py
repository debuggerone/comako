import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

import pytest
import pytest_asyncio
import asyncio
from tests.unit.test_config import TestAsyncSession, Base
from src.models.models import BalanceGroup, BalanceGroupMember, MarketParticipant
from src.services.balance_group import BalanceGroupRepository

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
async def test_create_balance_group(setup_database):
    async with TestAsyncSession() as session:
        repo = BalanceGroupRepository(session)
        group = await repo.create_balance_group("BG123", "Test Group")
        assert group.id == "BG123"
        assert group.name == "Test Group"
        await session.commit()

@pytest.mark.asyncio
async def test_get_balance_group(setup_database):
    async with TestAsyncSession() as session:
        repo = BalanceGroupRepository(session)
        # Create balance group first
        await repo.create_balance_group("BG123", "Test Group")
        group = await repo.get_balance_group("BG123")
        assert group is not None
        assert group.name == "Test Group"

@pytest.mark.asyncio
async def test_add_member(setup_database):
    async with TestAsyncSession() as session:
        repo = BalanceGroupRepository(session)
        # Create balance group and participant first
        await repo.create_balance_group("BG123", "Test Group")
        participant = MarketParticipant(id="MP456", name="Test Participant")
        session.add(participant)
        await session.commit()
        
        result = await repo.add_member("BG123", "MP456")
        assert result is not None
        assert result.balance_group_id == "BG123"
        assert result.market_participant_id == "MP456"
        await session.commit()

@pytest.mark.asyncio
async def test_remove_member(setup_database):
    async with TestAsyncSession() as session:
        repo = BalanceGroupRepository(session)
        # Create balance group and member first
        await repo.create_balance_group("BG123", "Test Group")
        participant = MarketParticipant(id="MP456", name="Test Participant")
        session.add(participant)
        await session.commit()
        await repo.add_member("BG123", "MP456")
        
        success = await repo.remove_member("BG123", "MP456")
        assert success is True
        await session.commit()

@pytest.mark.asyncio
async def test_get_members(setup_database):
    async with TestAsyncSession() as session:
        repo = BalanceGroupRepository(session)
        # Create balance group and member first
        await repo.create_balance_group("BG123", "Test Group")
        participant = MarketParticipant(id="MP456", name="Test Participant")
        session.add(participant)
        await session.commit()
        await repo.add_member("BG123", "MP456")
        
        members = await repo.get_members("BG123")
        assert len(members) == 1
        assert members[0].id == "MP456"
