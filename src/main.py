from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel, Field
from spectree import SpecTree, Response
from src.config import async_session
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from src.services.balance_group import BalanceGroupRepository
from src.services.energy_flow import EnergyFlowAggregator
from src.services.deviation import calculate_deviation, calculate_deviation_percentage
from src.services.settlement import calculate_settlement, calculate_settlement_with_percentage
from src.services.meter_reading import MeterReadingRepository
from src.services.anomaly_detection import AnomalyDetector
from src.services.aperak_generator import APERAKGenerator, validate_aperak_message
from src.models.models import BalanceGroup
from datetime import datetime
from typing import Optional, Dict, Any, List

# OpenAPI Documentation Configuration
api = SpecTree(
    "fastapi",
    title="CoMaKo API",
    description="Community Market Communication Platform for Energy Cooperatives",
    version="1.0.0",
)

app = FastAPI(
    title="CoMaKo API",
    description="Energy cooperative management system",
    version="1.0.0",
    openapi_url="/api/openapi.json",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
)

# Pydantic Models for API documentation
class HealthStatus(BaseModel):
    status: str
    db: str

class BalanceGroupResponse(BaseModel):
    id: str
    name: str

class BalanceGroupMemberResponse(BaseModel):
    balance_group_id: str
    participant_id: str

class SuccessResponse(BaseModel):
    success: bool

class ErrorResponse(BaseModel):
    error: str

class SettlementReportResponse(BaseModel):
    balance_group_id: str
    period: Dict[str, Optional[str]]
    aggregated_flows: Dict[str, float]
    deviations: Dict[str, float]
    settlements: Dict[str, float]

class MeterReadingResponse(BaseModel):
    id: str
    metering_point_id: str
    timestamp: str
    value_kwh: float
    reading_type: str
    created_at: str

class AnomalyResponse(BaseModel):
    anomalies_found: int
    analysis_period_days: int
    metering_point_filter: Optional[str]
    threshold_multiplier: float
    anomalies: List[Dict[str, Any]]

class AperakResponse(BaseModel):
    message_id: str
    aperak_message: str
    validation: Dict[str, bool]
    status: str
    generated_at: str

# Dependency to get DB session
async def get_db_session():
    async with async_session() as session:
        yield session

@app.get("/health", tags=["System"])
@api.validate(resp=Response(HTTP_200=HealthStatus))
async def health_check():
    """Check API and database connection status."""
    async with async_session() as session:
        await session.execute(text("SELECT 1"))
        return {"status": "ok", "db": "connected"}

@app.post("/balance_groups", tags=["Balance Groups"])
@api.validate(resp=Response(HTTP_200=BalanceGroupResponse))
async def create_balance_group(id: str, name: str, session: AsyncSession = Depends(get_db_session)):
    """Create a new balance group."""
    repo = BalanceGroupRepository(session)
    balance_group = await repo.create_balance_group(id, name)
    return {"id": balance_group.id, "name": balance_group.name}

@app.get("/balance_groups/{id}", tags=["Balance Groups"])
@api.validate(resp=Response(HTTP_200=BalanceGroupResponse, HTTP_404=ErrorResponse))
async def get_balance_group(id: str, session: AsyncSession = Depends(get_db_session)):
    """Get a balance group by ID."""
    repo = BalanceGroupRepository(session)
    balance_group = await repo.get_balance_group(id)
    if not balance_group:
        raise HTTPException(status_code=404, detail="Balance group not found")
    return {"id": balance_group.id, "name": balance_group.name}

@app.post("/balance_groups/{balance_group_id}/members/{participant_id}", tags=["Balance Groups"])
@api.validate(resp=Response(HTTP_200=BalanceGroupMemberResponse, HTTP_400=ErrorResponse))
async def add_member(balance_group_id: str, participant_id: str, session: AsyncSession = Depends(get_db_session)):
    """Add a market participant to a balance group."""
    repo = BalanceGroupRepository(session)
    member = await repo.add_member(balance_group_id, participant_id)
    if not member:
        raise HTTPException(status_code=400, detail="Failed to add member")
    return {"balance_group_id": member.balance_group_id, "participant_id": member.market_participant_id}

@app.delete("/balance_groups/{balance_group_id}/members/{participant_id}", tags=["Balance Groups"])
@api.validate(resp=Response(HTTP_200=SuccessResponse, HTTP_400=ErrorResponse))
async def remove_member(balance_group_id: str, participant_id: str, session: AsyncSession = Depends(get_db_session)):
    """Remove a market participant from a balance group."""
    repo = BalanceGroupRepository(session)
    success = await repo.remove_member(balance_group_id, participant_id)
    if not success:
        raise HTTPException(status_code=400, detail="Failed to remove member")
    return {"success": True}

@app.get("/balance_groups/{id}/report", tags=["Balance Groups"])
@api.validate(resp=Response(HTTP_200=SettlementReportResponse, HTTP_404=ErrorResponse))
async def generate_report(
    id: str, 
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    price_ct_per_kwh: int = 10,
    session: AsyncSession = Depends(get_db_session)
):
    """Generate a settlement report for a balance group"""
    # Get balance group
    repo = BalanceGroupRepository(session)
    balance_group = await repo.get_balance_group(id)
    if not balance_group:
        raise HTTPException(status_code=404, detail="Balance group not found")
    
    # Aggregate energy flows
    aggregator = EnergyFlowAggregator(session)
    aggregated = await aggregator.aggregate_energy_flows(id, start_time, end_time)
    
    # Calculate deviations and settlements
    consumption_deviation_kwh = calculate_deviation(aggregated['consumption_kwh'], aggregated['consumption_kwh'] * 0.95)  # Example forecast
    generation_deviation_kwh = calculate_deviation(aggregated['generation_kwh'], aggregated['generation_kwh'] * 1.05)    # Example forecast
    
    consumption_settlement = calculate_settlement(consumption_deviation_kwh, price_ct_per_kwh)
    generation_settlement = calculate_settlement(generation_deviation_kwh, price_ct_per_kwh)
    
    return {
        "balance_group_id": id,
        "period": {
            "start": start_time.isoformat() if start_time else None,
            "end": end_time.isoformat() if end_time else None
        },
        "aggregated_flows": aggregated,
        "deviations": {
            "consumption_kwh": consumption_deviation_kwh,
            "generation_kwh": generation_deviation_kwh
        },
        "settlements": {
            "consumption_eur": consumption_settlement,
            "generation_eur": generation_settlement
        }
    }

@app.get("/readings/{id}", tags=["Meter Readings"])
@api.validate(resp=Response(HTTP_200=MeterReadingResponse, HTTP_404=ErrorResponse))
async def get_reading(id: str, session: AsyncSession = Depends(get_db_session)):
    """Get a meter reading by ID."""
    repo = MeterReadingRepository(session)
    reading = await repo.get_reading(id)
    if not reading:
        raise HTTPException(status_code=404, detail="Reading not found")
    
    return {
        "id": reading.id,
        "metering_point_id": reading.metering_point_id,
        "timestamp": reading.timestamp.isoformat(),
        "value_kwh": reading.value_kwh,
        "reading_type": reading.reading_type,
        "created_at": reading.created_at.isoformat()
    }

@app.get("/readings/anomalies", tags=["Meter Readings"])
@api.validate(resp=Response(HTTP_200=AnomalyResponse))
async def get_anomalies(
    days: int = 7,
    metering_point_id: Optional[str] = None,
    threshold_multiplier: float = 2.0,
    session: AsyncSession = Depends(get_db_session)
):
    """Get anomalous meter readings using statistical analysis"""
    detector = AnomalyDetector(session)
    anomalies = await detector.get_recent_anomalies(days, metering_point_id)
    
    return {
        "anomalies_found": len(anomalies),
        "analysis_period_days": days,
        "metering_point_filter": metering_point_id,
        "threshold_multiplier": threshold_multiplier,
        "anomalies": anomalies
    }

@app.get("/edi/ack/{id}", tags=["EDI Gateway"])
@api.validate(resp=Response(HTTP_200=AperakResponse, HTTP_500=ErrorResponse))
async def generate_aperak(id: str):
    """Generate EDIFACT APERAK acknowledgment for a message."""
    try:
        # In a real implementation, you would:
        # 1. Look up the original message by ID from database
        # 2. Determine processing status
        # 3. Generate appropriate APERAK response
        
        # For demonstration, create a sample original message
        original_message = {
            "UNH": [id, "UTILMD", "D", "03B"],
            "UNB": ["UNOC:3", "SENDER123", "COMAKO", "250103:1200", "REF001"],
            "message_type": "UTILMD"
        }
        
        # Generate APERAK response
        generator = APERAKGenerator(sender_id="COMAKO")
        aperak_message = generator.generate_acceptance_aperak(original_message)
        
        # Validate the generated APERAK
        validation_results = validate_aperak_message(aperak_message)
        
        return {
            "message_id": id,
            "aperak_message": aperak_message,
            "validation": validation_results,
            "status": "accepted",
            "generated_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate APERAK: {str(e)}")

# Register the app with Spectree
api.register(app)
