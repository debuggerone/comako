import httpx
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from src.models.meter_reading import MeterReadingCreate, MeterReadingResponse


logger = logging.getLogger(__name__)


class MarketCoreClient:
    """Client for communicating with the Market Core service"""
    
    def __init__(self, base_url: str = "http://market_core", timeout: float = 30.0):
        """
        Initialize the Market Core client
        
        Args:
            base_url: Base URL of the Market Core service
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        
    async def send_to_market_core(self, reading: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send a meter reading to the Market Core settlement service
        
        Args:
            reading: Dictionary containing meter reading data
            
        Returns:
            Response from Market Core service
            
        Raises:
            httpx.HTTPError: If the request fails
            ValueError: If the response is invalid
        """
        url = f"{self.base_url}/settlement"
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                logger.info(f"Sending reading to Market Core: {reading.get('id', 'unknown')}")
                
                response = await client.post(
                    url,
                    json=reading,
                    headers={
                        "Content-Type": "application/json",
                        "User-Agent": "CoMaKo-MeterGateway/1.0"
                    }
                )
                
                response.raise_for_status()
                result = response.json()
                
                logger.info(f"Successfully sent reading to Market Core: {result}")
                return result
                
        except httpx.TimeoutException as e:
            logger.error(f"Timeout sending reading to Market Core: {e}")
            raise
        except httpx.HTTPError as e:
            logger.error(f"HTTP error sending reading to Market Core: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response from Market Core: {e}")
            raise ValueError("Invalid JSON response from Market Core")
    
    async def send_reading_batch(self, readings: list[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Send a batch of meter readings to Market Core
        
        Args:
            readings: List of meter reading dictionaries
            
        Returns:
            Batch processing response from Market Core
        """
        url = f"{self.base_url}/settlement/batch"
        
        batch_data = {
            "readings": readings,
            "batch_timestamp": datetime.utcnow().isoformat(),
            "batch_size": len(readings)
        }
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                logger.info(f"Sending batch of {len(readings)} readings to Market Core")
                
                response = await client.post(
                    url,
                    json=batch_data,
                    headers={
                        "Content-Type": "application/json",
                        "User-Agent": "CoMaKo-MeterGateway/1.0"
                    }
                )
                
                response.raise_for_status()
                result = response.json()
                
                logger.info(f"Successfully sent batch to Market Core: {result}")
                return result
                
        except httpx.TimeoutException as e:
            logger.error(f"Timeout sending batch to Market Core: {e}")
            raise
        except httpx.HTTPError as e:
            logger.error(f"HTTP error sending batch to Market Core: {e}")
            raise
    
    async def get_settlement_status(self, reading_id: str) -> Dict[str, Any]:
        """
        Get settlement status for a specific reading
        
        Args:
            reading_id: ID of the meter reading
            
        Returns:
            Settlement status information
        """
        url = f"{self.base_url}/settlement/status/{reading_id}"
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    url,
                    headers={"User-Agent": "CoMaKo-MeterGateway/1.0"}
                )
                
                response.raise_for_status()
                return response.json()
                
        except httpx.HTTPError as e:
            logger.error(f"Error getting settlement status: {e}")
            raise
    
    async def health_check(self) -> bool:
        """
        Check if Market Core service is healthy
        
        Returns:
            True if service is healthy, False otherwise
        """
        url = f"{self.base_url}/health"
        
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(url)
                response.raise_for_status()
                
                health_data = response.json()
                return health_data.get("status") == "ok"
                
        except Exception as e:
            logger.warning(f"Market Core health check failed: {e}")
            return False
    
    async def submit_energy_flow(self, balance_group_id: str, energy_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Submit aggregated energy flow data to Market Core
        
        Args:
            balance_group_id: ID of the balance group
            energy_data: Aggregated energy flow data
            
        Returns:
            Response from Market Core
        """
        url = f"{self.base_url}/energy_flows"
        
        payload = {
            "balance_group_id": balance_group_id,
            "timestamp": datetime.utcnow().isoformat(),
            **energy_data
        }
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    url,
                    json=payload,
                    headers={
                        "Content-Type": "application/json",
                        "User-Agent": "CoMaKo-MeterGateway/1.0"
                    }
                )
                
                response.raise_for_status()
                return response.json()
                
        except httpx.HTTPError as e:
            logger.error(f"Error submitting energy flow: {e}")
            raise
    
    async def get_balance_group_report(self, balance_group_id: str, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Get settlement report for a balance group from Market Core
        
        Args:
            balance_group_id: ID of the balance group
            start_date: Optional start date for the report
            end_date: Optional end date for the report
            
        Returns:
            Settlement report data
        """
        url = f"{self.base_url}/balance_groups/{balance_group_id}/report"
        
        params = {}
        if start_date:
            params["start_time"] = start_date.isoformat()
        if end_date:
            params["end_time"] = end_date.isoformat()
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    url,
                    params=params,
                    headers={"User-Agent": "CoMaKo-MeterGateway/1.0"}
                )
                
                response.raise_for_status()
                return response.json()
                
        except httpx.HTTPError as e:
            logger.error(f"Error getting balance group report: {e}")
            raise


class MarketCoreIntegration:
    """High-level integration service for Market Core communication"""
    
    def __init__(self, client: MarketCoreClient):
        self.client = client
    
    async def process_meter_reading(self, reading: MeterReadingResponse) -> Dict[str, Any]:
        """
        Process a meter reading through Market Core integration
        
        Args:
            reading: Meter reading response object
            
        Returns:
            Processing result
        """
        # Convert reading to dictionary format expected by Market Core
        reading_data = {
            "id": reading.id,
            "metering_point_id": reading.metering_point_id,
            "timestamp": reading.timestamp.isoformat(),
            "value_kwh": reading.value_kwh,
            "reading_type": reading.reading_type,
            "source": reading.source,
            "created_at": reading.created_at.isoformat()
        }
        
        try:
            # Send to Market Core
            result = await self.client.send_to_market_core(reading_data)
            
            # Log successful processing
            logger.info(f"Successfully processed reading {reading.id} through Market Core")
            
            return {
                "status": "success",
                "reading_id": reading.id,
                "market_core_response": result
            }
            
        except Exception as e:
            logger.error(f"Failed to process reading {reading.id} through Market Core: {e}")
            return {
                "status": "error",
                "reading_id": reading.id,
                "error": str(e)
            }
    
    async def is_market_core_available(self) -> bool:
        """Check if Market Core is available for processing"""
        return await self.client.health_check()


# Default client instance
default_client = MarketCoreClient()
default_integration = MarketCoreIntegration(default_client)
