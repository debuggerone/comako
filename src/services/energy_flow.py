from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from src.models.models import EnergyReading, BalanceGroupMember, MarketParticipant, MeteringPoint
from datetime import datetime
from typing import Optional, Dict, Any

async def aggregate_energy_flows(balance_group_id: str, session: AsyncSession):
    """
    Aggregates energy flows for a balance group by summing consumption and generation values.
    
    Args:
        balance_group_id: ID of the balance group to aggregate
        session: Async database session
        
    Returns:
        Dict containing total consumption, generation, and net flow values in kWh
    """
    # Query all energy readings for the balance group through its members
    result = await session.execute(
        select(EnergyReading)
        .join(MeteringPoint, EnergyReading.metering_point_id == MeteringPoint.id)
        .join(MarketParticipant, MeteringPoint.market_participant_id == MarketParticipant.id)
        .join(BalanceGroupMember, MarketParticipant.id == BalanceGroupMember.market_participant_id)
        .filter(BalanceGroupMember.balance_group_id == balance_group_id)
    )
    readings = result.scalars().all()
    
    # Calculate totals based on reading direction
    total_consumption = sum(r.value_kwh for r in readings if r.direction == "consumption")
    total_generation = sum(r.value_kwh for r in readings if r.direction == "generation")
    
    return {
        "balance_group_id": balance_group_id,
        "total_consumption": total_consumption,
        "total_generation": total_generation,
        "net_flow": total_generation - total_consumption
    }

class EnergyFlowAggregator:
    """
    Aggregates energy flow data for balance groups.
    """
    def __init__(self, session: AsyncSession):
        self.session = session

    async def aggregate_energy_flows(
        self,
        balance_group_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Dict[str, float]:
        """
        Aggregates energy flows for a specific balance group within a given time range.

        Args:
            balance_group_id: The ID of the balance group.
            start_time: The start of the time range for aggregation.
            end_time: The end of the time range for aggregation.

        Returns:
            A dictionary containing aggregated consumption and generation values.
        """
        # Base query to select energy readings for the balance group
        query = (
            select(
                EnergyReading.reading_type,
                func.sum(EnergyReading.value_kwh).label("total_kwh")
            )
            .join(MeteringPoint, EnergyReading.metering_point_id == MeteringPoint.id)
            .join(BalanceGroupMember, MeteringPoint.id == BalanceGroupMember.metering_point_id)
            .filter(BalanceGroupMember.balance_group_id == balance_group_id)
            .group_by(EnergyReading.reading_type)
        )

        # Apply time filters if provided
        if start_time:
            query = query.filter(EnergyReading.timestamp >= start_time)
        if end_time:
            query = query.filter(EnergyReading.timestamp <= end_time)

        result = await self.session.execute(query)
        rows = result.all()

        # Process results
        aggregated_data = {"consumption_kwh": 0.0, "generation_kwh": 0.0}
        for row in rows:
            if row.reading_type == "consumption":
                aggregated_data["consumption_kwh"] = row.total_kwh or 0.0
            elif row.reading_type == "generation":
                aggregated_data["generation_kwh"] = row.total_kwh or 0.0
        
        return aggregated_data
