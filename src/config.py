from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
import os
import aio_pika
from typing import Optional
import logging

logger = logging.getLogger(__name__)

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://comako:comako@localhost:5432/comako")

engine = create_async_engine(
    DATABASE_URL,
    echo=True
)
async_session = async_sessionmaker(engine, expire_on_commit=False)

# RabbitMQ configuration
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://comako:comako@localhost:5672/")

# Global connection pool
_rabbit_connection: Optional[aio_pika.abc.AbstractRobustConnection] = None


async def get_rabbit_connection() -> aio_pika.abc.AbstractRobustConnection:
    """
    Get or create a RabbitMQ connection.
    
    Returns:
        Robust RabbitMQ connection that automatically reconnects
    """
    global _rabbit_connection
    
    if _rabbit_connection is None or _rabbit_connection.is_closed:
        try:
            _rabbit_connection = await aio_pika.connect_robust(
                RABBITMQ_URL,
                heartbeat=600,
                blocked_connection_timeout=300,
            )
            logger.info("Connected to RabbitMQ")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise
    
    return _rabbit_connection


async def close_rabbit_connection():
    """Close the RabbitMQ connection."""
    global _rabbit_connection
    
    if _rabbit_connection and not _rabbit_connection.is_closed:
        await _rabbit_connection.close()
        _rabbit_connection = None
        logger.info("Closed RabbitMQ connection")


async def setup_message_queues():
    """
    Setup message queues and exchanges for the application.
    
    Creates:
    - settlement_queue: For meter reading -> settlement flow
    - edi_processing_queue: For EDI message processing
    - aperak_queue: For APERAK response handling
    """
    try:
        connection = await get_rabbit_connection()
        channel = await connection.channel()
        
        # Declare queues
        settlement_queue = await channel.declare_queue(
            "settlement_queue",
            durable=True,
            arguments={"x-message-ttl": 86400000}  # 24 hours TTL
        )
        
        edi_processing_queue = await channel.declare_queue(
            "edi_processing_queue",
            durable=True,
            arguments={"x-message-ttl": 86400000}  # 24 hours TTL
        )
        
        aperak_queue = await channel.declare_queue(
            "aperak_queue",
            durable=True,
            arguments={"x-message-ttl": 86400000}  # 24 hours TTL
        )
        
        # Declare exchange for routing
        exchange = await channel.declare_exchange(
            "comako_exchange",
            aio_pika.ExchangeType.TOPIC,
            durable=True
        )
        
        # Bind queues to exchange with routing keys
        await settlement_queue.bind(exchange, "meter.reading.*")
        await edi_processing_queue.bind(exchange, "edi.message.*")
        await aperak_queue.bind(exchange, "edi.aperak.*")
        
        await channel.close()
        logger.info("Message queues setup completed")
        
    except Exception as e:
        logger.error(f"Failed to setup message queues: {e}")
        raise


# Message publishing utilities
async def publish_message(routing_key: str, message_body: dict, exchange_name: str = "comako_exchange"):
    """
    Publish a message to RabbitMQ.
    
    Args:
        routing_key: Routing key for message routing
        message_body: Message payload as dictionary
        exchange_name: Exchange to publish to
    """
    import json
    
    try:
        connection = await get_rabbit_connection()
        channel = await connection.channel()
        
        exchange = await channel.get_exchange(exchange_name)
        
        message = aio_pika.Message(
            body=json.dumps(message_body).encode(),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        )
        
        await exchange.publish(message, routing_key=routing_key)
        await channel.close()
        
        logger.info(f"Published message to {routing_key}")
        
    except Exception as e:
        logger.error(f"Failed to publish message: {e}")
        raise
