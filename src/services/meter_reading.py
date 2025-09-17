from src.models.models import EnergyReading
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from datetime import datetime
from typing import Optional, List, Dict, Any
import uuid
import logging

logger = logging.getLogger(__name__)


class MeterReadingRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_reading(self, id: str) -> Optional[EnergyReading]:
        """Get a meter reading by ID"""
        return await self.session.get(EnergyReading, id)

    async def create_reading(
        self,
        metering_point_id: str,
        value_kwh: float,
        reading_type: str = "consumption",
        timestamp: Optional[datetime] = None
    ) -> EnergyReading:
        """Create a new meter reading"""
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        reading = EnergyReading(
            id=str(uuid.uuid4()),
            metering_point_id=metering_point_id,
            timestamp=timestamp,
            value_kwh=value_kwh,
            reading_type=reading_type,
            created_at=datetime.utcnow()
        )
        
        self.session.add(reading)
        await self.session.commit()
        await self.session.refresh(reading)
        
        # Publish reading to message queue for settlement processing
        await self.publish_reading(reading)
        
        return reading

    async def publish_reading(self, reading: EnergyReading):
        """
        Publish meter reading to RabbitMQ for settlement processing.
        
        Args:
            reading: The meter reading to publish
        """
        try:
            # Import here to avoid circular imports
            from src.config import publish_message
            
            # Create message payload
            message_payload = {
                "reading_id": reading.id,
                "metering_point_id": reading.metering_point_id,
                "timestamp": reading.timestamp.isoformat(),
                "value_kwh": reading.value_kwh,
                "reading_type": reading.reading_type,
                "created_at": reading.created_at.isoformat(),
                "event_type": "meter_reading_created"
            }
            
            # Publish to settlement queue
            await publish_message(
                routing_key="meter.reading.created",
                message_body=message_payload
            )
            
            logger.info(f"Published meter reading {reading.id} to settlement queue")
            
        except Exception as e:
            logger.error(f"Failed to publish meter reading {reading.id}: {e}")
            # Don't raise exception to avoid breaking the reading creation
            # In production, you might want to implement retry logic

    async def get_readings_by_metering_point(
        self,
        metering_point_id: str,
        limit: int = 100
    ) -> List[EnergyReading]:
        """Get recent readings for a specific metering point"""
        result = await self.session.execute(
            select(EnergyReading)
            .where(EnergyReading.metering_point_id == metering_point_id)
            .order_by(EnergyReading.timestamp.desc())
            .limit(limit)
        )
        return result.scalars().all()

    async def get_readings_in_period(
        self,
        start_time: datetime,
        end_time: datetime,
        metering_point_id: Optional[str] = None
    ) -> List[EnergyReading]:
        """Get readings within a specific time period"""
        query = select(EnergyReading).where(
            EnergyReading.timestamp >= start_time,
            EnergyReading.timestamp <= end_time
        )
        
        if metering_point_id:
            query = query.where(EnergyReading.metering_point_id == metering_point_id)
        
        query = query.order_by(EnergyReading.timestamp.desc())
        
        result = await self.session.execute(query)
        return result.scalars().all()


# Message consumer for settlement processing
class SettlementMessageConsumer:
    """
    Consumer for processing meter reading messages from RabbitMQ.
    This would typically run as a separate service or background task.
    """
    
    def __init__(self, session: AsyncSession):
        self.session = session

    async def process_meter_reading_message(self, message_body: Dict[str, Any]):
        """
        Process a meter reading message for settlement calculation.
        
        Args:
            message_body: The message payload from RabbitMQ
        """
        try:
            reading_id = message_body.get("reading_id")
            metering_point_id = message_body.get("metering_point_id")
            value_kwh = message_body.get("value_kwh")
            
            logger.info(f"Processing meter reading {reading_id} for settlement")
            
            # Here you would:
            # 1. Validate the reading
            # 2. Find the associated balance group
            # 3. Trigger settlement calculation
            # 4. Update settlement records
            
            # For now, we'll simulate the processing
            await self._trigger_settlement_calculation(
                metering_point_id=metering_point_id,
                reading_value=value_kwh
            )
            
            logger.info(f"Successfully processed meter reading {reading_id}")
            
        except Exception as e:
            logger.error(f"Failed to process meter reading message: {e}")
            raise

    async def _trigger_settlement_calculation(
        self,
        metering_point_id: str,
        reading_value: float
    ):
        """
        Trigger settlement calculation for a meter reading.
        
        This is a simplified version - in production this would:
        - Find the balance group for the metering point
        - Calculate deviations from forecasts
        - Update settlement records
        - Generate settlement reports
        """
        # Import settlement services
        from src.services.settlement import calculate_settlement
        from src.services.deviation import calculate_deviation
        
        # Simulate forecast value (in production, this would come from database)
        forecast_value = reading_value * 0.95  # Assume 5% under-forecast
        
        # Calculate deviation
        deviation = calculate_deviation(reading_value, forecast_value)
        
        # Calculate settlement (using default price)
        settlement_amount = calculate_settlement(deviation, price_ct_per_kwh=10)
        
        logger.info(
            f"Settlement calculated for {metering_point_id}: "
            f"deviation={deviation} kWh, settlement={settlement_amount} EUR"
        )
        
        # In production, you would save this to the database
        # and potentially publish another message for reporting


async def setup_settlement_consumer():
    """
    Setup the RabbitMQ consumer for settlement processing.
    This would typically be called during application startup.
    """
    try:
        from src.config import get_rabbit_connection, async_session
        
        connection = await get_rabbit_connection()
        channel = await connection.channel()
        
        # Get the settlement queue
        settlement_queue = await channel.get_queue("settlement_queue")
        
        async def message_handler(message):
            """Handle incoming settlement messages"""
            import json
            
            async with async_session() as session:
                consumer = SettlementMessageConsumer(session)
                
                try:
                    message_body = json.loads(message.body.decode())
                    await consumer.process_meter_reading_message(message_body)
                    await message.ack()
                    
                except Exception as e:
                    logger.error(f"Failed to process message: {e}")
                    await message.nack(requeue=True)
        
        # Start consuming messages
        await settlement_queue.consume(message_handler)
        
        logger.info("Settlement message consumer started")
        
    except Exception as e:
        logger.error(f"Failed to setup settlement consumer: {e}")
        raise
