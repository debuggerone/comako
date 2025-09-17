import statistics
from typing import List, Dict, Any
from src.models.models import EnergyReading
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from datetime import datetime, timedelta


class AnomalyDetector:
    def __init__(self, session: AsyncSession):
        self.session = session

    def detect_anomalies(self, readings: List[Dict[str, Any]], threshold_multiplier: float = 2.0) -> List[Dict[str, Any]]:
        """
        Detect anomalies in meter readings using statistical analysis.
        Uses standard deviation to identify outliers.
        
        Args:
            readings: List of reading dictionaries with 'value_kwh' field
            threshold_multiplier: Number of standard deviations to consider as anomaly threshold
            
        Returns:
            List of readings that are considered anomalies
        """
        if len(readings) < 3:
            # Need at least 3 readings for meaningful statistical analysis
            return []
        
        values = [reading['value_kwh'] for reading in readings]
        
        # Calculate statistical measures
        mean_value = statistics.mean(values)
        stdev_value = statistics.stdev(values) if len(values) > 1 else 0
        
        # Define anomaly threshold
        upper_threshold = mean_value + (threshold_multiplier * stdev_value)
        lower_threshold = mean_value - (threshold_multiplier * stdev_value)
        
        # Identify anomalies
        anomalies = []
        for reading in readings:
            value = reading['value_kwh']
            if value > upper_threshold or value < lower_threshold:
                # Add anomaly metadata
                anomaly_reading = reading.copy()
                anomaly_reading['anomaly_type'] = 'high' if value > upper_threshold else 'low'
                anomaly_reading['deviation_from_mean'] = abs(value - mean_value)
                anomaly_reading['threshold_exceeded'] = value > upper_threshold if value > mean_value else value < lower_threshold
                anomalies.append(anomaly_reading)
        
        return anomalies

    async def get_recent_anomalies(self, days: int = 7, metering_point_id: str = None) -> List[Dict[str, Any]]:
        """
        Get anomalies from recent meter readings.
        
        Args:
            days: Number of days to look back for readings
            metering_point_id: Optional filter for specific metering point
            
        Returns:
            List of anomalous readings
        """
        # Calculate date range
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=days)
        
        # Build query
        query = select(EnergyReading).where(
            EnergyReading.timestamp >= start_date,
            EnergyReading.timestamp <= end_date
        )
        
        if metering_point_id:
            query = query.where(EnergyReading.metering_point_id == metering_point_id)
        
        # Execute query
        result = await self.session.execute(query)
        readings = result.scalars().all()
        
        # Convert to dictionaries
        reading_dicts = []
        for reading in readings:
            reading_dicts.append({
                'id': reading.id,
                'metering_point_id': reading.metering_point_id,
                'timestamp': reading.timestamp.isoformat(),
                'value_kwh': reading.value_kwh,
                'reading_type': reading.reading_type,
                'created_at': reading.created_at.isoformat()
            })
        
        # Detect anomalies
        return self.detect_anomalies(reading_dicts)

    def is_outlier(self, reading: Dict[str, Any], reference_readings: List[Dict[str, Any]], threshold_multiplier: float = 2.0) -> bool:
        """
        Check if a single reading is an outlier compared to reference readings.
        
        Args:
            reading: The reading to check
            reference_readings: List of reference readings for comparison
            threshold_multiplier: Number of standard deviations to consider as outlier threshold
            
        Returns:
            True if the reading is an outlier, False otherwise
        """
        if len(reference_readings) < 2:
            return False
        
        values = [r['value_kwh'] for r in reference_readings]
        mean_value = statistics.mean(values)
        stdev_value = statistics.stdev(values)
        
        upper_threshold = mean_value + (threshold_multiplier * stdev_value)
        lower_threshold = mean_value - (threshold_multiplier * stdev_value)
        
        reading_value = reading['value_kwh']
        return reading_value > upper_threshold or reading_value < lower_threshold
