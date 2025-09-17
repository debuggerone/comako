from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from src.models.models import BalanceGroup, BalanceGroupMember, MarketParticipant
from typing import List, Optional

class BalanceGroupRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create_balance_group(self, id: str, name: str) -> BalanceGroup:
        """Create a new balance group"""
        balance_group = BalanceGroup(id=id, name=name)
        self.session.add(balance_group)
        await self.session.commit()
        await self.session.refresh(balance_group)
        return balance_group

    async def get_balance_group(self, id: str) -> Optional[BalanceGroup]:
        """Retrieve a balance group by ID"""
        result = await self.session.execute(select(BalanceGroup).where(BalanceGroup.id == id))
        return result.scalar_one_or_none()

    async def add_member(self, balance_group_id: str, participant_id: str) -> Optional[BalanceGroupMember]:
        """Add a market participant to a balance group"""
        # Check if balance group exists
        balance_group = await self.get_balance_group(balance_group_id)
        if not balance_group:
            return None

        # Check if participant exists
        result = await self.session.execute(select(MarketParticipant).where(MarketParticipant.id == participant_id))
        participant = result.scalar_one_or_none()
        if not participant:
            return None

        # Create the membership
        member = BalanceGroupMember(balance_group_id=balance_group_id, market_participant_id=participant_id)
        self.session.add(member)
        await self.session.commit()
        await self.session.refresh(member)
        return member

    async def remove_member(self, balance_group_id: str, participant_id: str) -> bool:
        """Remove a market participant from a balance group"""
        result = await self.session.execute(
            select(BalanceGroupMember).where(
                BalanceGroupMember.balance_group_id == balance_group_id,
                BalanceGroupMember.market_participant_id == participant_id
            )
        )
        member = result.scalar_one_or_none()
        if member:
            await self.session.delete(member)
            await self.session.commit()
            return True
        return False

    async def get_members(self, balance_group_id: str) -> List[MarketParticipant]:
        """Get all members of a balance group"""
        result = await self.session.execute(
            select(MarketParticipant)
            .join(BalanceGroupMember)
            .where(BalanceGroupMember.balance_group_id == balance_group_id)
        )
        return result.scalars().all()
