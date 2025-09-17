"""
EDI Processing Service

Handles the processing and publishing of parsed EDI messages to RabbitMQ
for consumption by Market Core and other services.
"""

import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class EDIProcessor:
    """
    Service for processing and publishing EDI messages through the message bus.
    """
    
    def __init__(self):
        self.processor_id = "edi_processor"
    
    async def publish_parsed_edi(self, parsed_data: Dict[str, Any], message_type: str = "UTILMD") -> bool:
        """
        Publish parsed EDI data to the message queue for processing.
        
        Args:
            parsed_data: The parsed EDI data structure
            message_type: Type of EDI message (UTILMD, MSCONS, etc.)
            
        Returns:
            bool: True if published successfully, False otherwise
        """
        try:
            # Import here to avoid circular imports
            from src.config import publish_message
            
            # Create message payload
            message_payload = {
                "message_id": self._extract_message_id(parsed_data),
                "message_type": message_type,
                "sender_id": self._extract_sender_id(parsed_data),
                "recipient_id": self._extract_recipient_id(parsed_data),
                "timestamp": datetime.utcnow().isoformat(),
                "parsed_data": parsed_data,
                "processing_status": "pending",
                "event_type": "edi_message_received"
            }
            
            # Determine routing key based on message type
            routing_key = f"edi.{message_type.lower()}.received"
            
            # Publish to EDI processing queue
            await publish_message(
                routing_key=routing_key,
                message_body=message_payload
            )
            
            logger.info(f"Published EDI message {message_payload['message_id']} to queue")
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish EDI message: {e}")
            return False
    
    async def publish_aperak_response(self, aperak_message: str, original_message_id: str) -> bool:
        """
        Publish APERAK response message.
        
        Args:
            aperak_message: The generated APERAK message
            original_message_id: ID of the original message being acknowledged
            
        Returns:
            bool: True if published successfully, False otherwise
        """
        try:
            from src.config import publish_message
            
            message_payload = {
                "aperak_message": aperak_message,
                "original_message_id": original_message_id,
                "timestamp": datetime.utcnow().isoformat(),
                "event_type": "aperak_generated"
            }
            
            await publish_message(
                routing_key="edi.aperak.generated",
                message_body=message_payload
            )
            
            logger.info(f"Published APERAK response for message {original_message_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish APERAK response: {e}")
            return False
    
    def _extract_message_id(self, parsed_data: Dict[str, Any]) -> str:
        """Extract message ID from parsed EDI data."""
        if "UNH" in parsed_data and isinstance(parsed_data["UNH"], list):
            return parsed_data["UNH"][0] if len(parsed_data["UNH"]) > 0 else "unknown"
        return f"msg_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    
    def _extract_sender_id(self, parsed_data: Dict[str, Any]) -> str:
        """Extract sender ID from parsed EDI data."""
        if "UNB" in parsed_data and isinstance(parsed_data["UNB"], list):
            return parsed_data["UNB"][1] if len(parsed_data["UNB"]) > 1 else "unknown"
        return "unknown"
    
    def _extract_recipient_id(self, parsed_data: Dict[str, Any]) -> str:
        """Extract recipient ID from parsed EDI data."""
        if "UNB" in parsed_data and isinstance(parsed_data["UNB"], list):
            return parsed_data["UNB"][2] if len(parsed_data["UNB"]) > 2 else "COMAKO"
        return "COMAKO"


class EDIMessageConsumer:
    """
    Consumer for processing EDI messages from RabbitMQ.
    This would typically run as a separate service or background task.
    """
    
    def __init__(self, session):
        self.session = session
        self.processor = EDIProcessor()
    
    async def process_edi_message(self, message_body: Dict[str, Any]):
        """
        Process an EDI message from the queue.
        
        Args:
            message_body: The message payload from RabbitMQ
        """
        try:
            message_id = message_body.get("message_id")
            message_type = message_body.get("message_type")
            parsed_data = message_body.get("parsed_data")
            
            logger.info(f"Processing EDI message {message_id} of type {message_type}")
            
            # Process based on message type
            if message_type == "UTILMD":
                await self._process_utilmd_message(parsed_data, message_id)
            elif message_type == "MSCONS":
                await self._process_mscons_message(parsed_data, message_id)
            else:
                logger.warning(f"Unknown message type: {message_type}")
            
            # Generate and publish APERAK response
            await self._generate_aperak_response(parsed_data, message_id)
            
            logger.info(f"Successfully processed EDI message {message_id}")
            
        except Exception as e:
            logger.error(f"Failed to process EDI message: {e}")
            raise
    
    async def _process_utilmd_message(self, parsed_data: Dict[str, Any], message_id: str):
        """Process UTILMD (Utilities Master Data) message."""
        # Extract metering point data
        metering_points = self._extract_metering_points(parsed_data)
        
        for mp_data in metering_points:
            # Here you would typically:
            # 1. Validate metering point data
            # 2. Update database records
            # 3. Trigger related business processes
            logger.info(f"Processing metering point: {mp_data.get('id', 'unknown')}")
    
    async def _process_mscons_message(self, parsed_data: Dict[str, Any], message_id: str):
        """Process MSCONS (Metered Services Consumption Report) message."""
        # Extract consumption data
        consumption_data = self._extract_consumption_data(parsed_data)
        
        for reading in consumption_data:
            # Here you would typically:
            # 1. Validate consumption data
            # 2. Store readings in database
            # 3. Trigger settlement calculations
            logger.info(f"Processing consumption reading: {reading.get('value', 'unknown')} kWh")
    
    async def _generate_aperak_response(self, parsed_data: Dict[str, Any], message_id: str):
        """Generate and publish APERAK response."""
        try:
            from src.services.aperak_generator import APERAKGenerator
            
            generator = APERAKGenerator(sender_id="COMAKO")
            aperak_message = generator.generate_acceptance_aperak(parsed_data)
            
            # Publish APERAK response
            await self.processor.publish_aperak_response(aperak_message, message_id)
            
        except Exception as e:
            logger.error(f"Failed to generate APERAK for message {message_id}: {e}")
    
    def _extract_metering_points(self, parsed_data: Dict[str, Any]) -> list:
        """Extract metering point data from parsed EDI."""
        metering_points = []
        
        # Extract from LOC segments
        if "LOC" in parsed_data:
            loc_data = parsed_data["LOC"]
            if isinstance(loc_data, list) and len(loc_data) >= 2:
                metering_points.append({
                    "id": loc_data[1],
                    "description": loc_data[2] if len(loc_data) > 2 else ""
                })
        
        return metering_points
    
    def _extract_consumption_data(self, parsed_data: Dict[str, Any]) -> list:
        """Extract consumption data from parsed EDI."""
        consumption_data = []
        
        # Extract from QTY segments
        if "QTY" in parsed_data:
            qty_data = parsed_data["QTY"]
            if isinstance(qty_data, list) and len(qty_data) >= 2:
                consumption_data.append({
                    "value": float(qty_data[1]) if qty_data[1].replace('.', '').isdigit() else 0.0,
                    "unit": qty_data[2] if len(qty_data) > 2 else "KWH"
                })
        
        return consumption_data


async def setup_edi_consumer():
    """
    Setup the RabbitMQ consumer for EDI processing.
    This would typically be called during application startup.
    """
    try:
        from src.config import get_rabbit_connection, async_session
        
        connection = await get_rabbit_connection()
        channel = await connection.channel()
        
        # Get the EDI processing queue
        edi_queue = await channel.get_queue("edi_processing_queue")
        
        async def message_handler(message):
            """Handle incoming EDI messages"""
            import json
            
            async with async_session() as session:
                consumer = EDIMessageConsumer(session)
                
                try:
                    message_body = json.loads(message.body.decode())
                    await consumer.process_edi_message(message_body)
                    await message.ack()
                    
                except Exception as e:
                    logger.error(f"Failed to process EDI message: {e}")
                    await message.nack(requeue=True)
        
        # Start consuming messages
        await edi_queue.consume(message_handler)
        
        logger.info("EDI message consumer started")
        
    except Exception as e:
        logger.error(f"Failed to setup EDI consumer: {e}")
        raise


# Convenience functions for integration
async def process_and_publish_edi(edi_content: str, message_type: str = "UTILMD") -> bool:
    """
    Complete EDI processing pipeline: parse, convert, and publish.
    
    Args:
        edi_content: Raw EDI message content
        message_type: Type of EDI message
        
    Returns:
        bool: True if processed successfully, False otherwise
    """
    try:
        from src.services.edi_parser import EDIFACTParser
        from src.services.edi_converter import convert_edi_to_json
        
        # Parse EDI content
        parser = EDIFACTParser()
        parsed_data = parser.parse_edi_file(edi_content)
        
        # Convert to JSON for easier processing
        json_data = convert_edi_to_json(parsed_data)
        
        # Publish to message queue
        processor = EDIProcessor()
        success = await processor.publish_parsed_edi(parsed_data, message_type)
        
        return success
        
    except Exception as e:
        logger.error(f"Failed to process and publish EDI: {e}")
        return False
