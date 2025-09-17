from pydantic import BaseModel, Field, validator
from datetime import datetime
from typing import Optional, Literal
from enum import Enum


class ReadingSource(str, Enum):
    """Enumeration of possible meter reading sources"""
    CSV = "CSV"
    VOICEBOT = "voicebot"
    MANUAL = "manual"
    API = "api"
    EDI = "edi"
    SMART_METER = "smart_meter"


class ReadingType(str, Enum):
    """Enumeration of meter reading types"""
    CONSUMPTION = "consumption"
    GENERATION = "generation"
    NET = "net"


class MeterReadingCreate(BaseModel):
    """Pydantic model for creating new meter readings"""
    metering_point: str = Field(..., min_length=1, max_length=50, description="Metering point identifier")
    timestamp: datetime = Field(..., description="Timestamp of the meter reading")
    value_kwh: float = Field(..., ge=0, description="Energy value in kWh (must be non-negative)")
    source: ReadingSource = Field(..., description="Source of the meter reading")
    reading_type: ReadingType = Field(default=ReadingType.CONSUMPTION, description="Type of meter reading")
    
    @validator('timestamp')
    def validate_timestamp(cls, v):
        """Ensure timestamp is not in the future"""
        if v > datetime.utcnow():
            raise ValueError('Timestamp cannot be in the future')
        return v
    
    @validator('value_kwh')
    def validate_value_kwh(cls, v):
        """Validate energy value is reasonable"""
        if v < 0:
            raise ValueError('Energy value cannot be negative')
        if v > 1000000:  # 1 million kWh seems unreasonable for a single reading
            raise ValueError('Energy value seems unreasonably high')
        return v
    
    @validator('metering_point')
    def validate_metering_point(cls, v):
        """Validate metering point format"""
        if not v.strip():
            raise ValueError('Metering point cannot be empty')
        # Basic format validation - could be extended based on specific requirements
        if len(v.strip()) < 3:
            raise ValueError('Metering point must be at least 3 characters long')
        return v.strip()

    class Config:
        """Pydantic configuration"""
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class MeterReadingResponse(BaseModel):
    """Pydantic model for meter reading responses"""
    id: str
    metering_point_id: str
    timestamp: datetime
    value_kwh: float
    reading_type: str
    source: Optional[str] = None
    created_at: datetime
    
    class Config:
        """Pydantic configuration"""
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class MeterReadingBatch(BaseModel):
    """Pydantic model for batch meter reading submissions"""
    readings: list[MeterReadingCreate] = Field(..., min_items=1, max_items=1000, description="List of meter readings")
    batch_source: ReadingSource = Field(..., description="Source of the batch")
    batch_timestamp: datetime = Field(default_factory=datetime.utcnow, description="Timestamp when batch was created")
    
    @validator('readings')
    def validate_readings_uniqueness(cls, v):
        """Ensure no duplicate readings in the batch"""
        seen = set()
        for reading in v:
            key = (reading.metering_point, reading.timestamp)
            if key in seen:
                raise ValueError(f'Duplicate reading found for metering point {reading.metering_point} at {reading.timestamp}')
            seen.add(key)
        return v

    class Config:
        """Pydantic configuration"""
        use_enum_values = True


class MeterReadingFilter(BaseModel):
    """Pydantic model for filtering meter readings"""
    metering_point_id: Optional[str] = Field(None, description="Filter by metering point ID")
    start_date: Optional[datetime] = Field(None, description="Start date for filtering")
    end_date: Optional[datetime] = Field(None, description="End date for filtering")
    reading_type: Optional[ReadingType] = Field(None, description="Filter by reading type")
    source: Optional[ReadingSource] = Field(None, description="Filter by reading source")
    min_value: Optional[float] = Field(None, ge=0, description="Minimum energy value filter")
    max_value: Optional[float] = Field(None, ge=0, description="Maximum energy value filter")
    
    @validator('end_date')
    def validate_date_range(cls, v, values):
        """Ensure end_date is after start_date"""
        if v and 'start_date' in values and values['start_date']:
            if v <= values['start_date']:
                raise ValueError('End date must be after start date')
        return v
    
    @validator('max_value')
    def validate_value_range(cls, v, values):
        """Ensure max_value is greater than min_value"""
        if v and 'min_value' in values and values['min_value']:
            if v <= values['min_value']:
                raise ValueError('Maximum value must be greater than minimum value')
        return v

    class Config:
        """Pydantic configuration"""
        use_enum_values = True


class ValidationError(BaseModel):
    """Pydantic model for validation error responses"""
    field: str
    message: str
    invalid_value: Optional[str] = None


class MeterReadingValidationResponse(BaseModel):
    """Pydantic model for validation responses"""
    valid: bool
    errors: list[ValidationError] = []
    warnings: list[str] = []
    
    class Config:
        """Pydantic configuration"""
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
