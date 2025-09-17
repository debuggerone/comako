from typing import Dict, List, Any, Optional
from datetime import datetime
import logging


logger = logging.getLogger(__name__)


class SegmentHandler:
    """Base class for EDI segment handlers"""
    
    def __init__(self):
        self.segment_type = None
    
    def handle(self, segment: Dict[str, Any]) -> Dict[str, Any]:
        """Handle a segment and return processed data"""
        raise NotImplementedError("Subclasses must implement handle method")
    
    def validate_segment(self, segment: Dict[str, Any]) -> bool:
        """Validate segment structure"""
        return segment.get("tag") == self.segment_type


class QTYHandler(SegmentHandler):
    """Handler for QTY (Quantity) segments"""
    
    def __init__(self):
        super().__init__()
        self.segment_type = "QTY"
    
    def handle_QTY(self, segment: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract quantity data from QTY segment
        
        QTY segment format: QTY+qualifier:value:unit
        """
        elements = segment.get("elements", [])
        
        if not elements:
            logger.warning("QTY segment has no elements")
            return {}
        
        # First element should contain quantity details
        qty_details = elements[0] if elements else []
        
        if isinstance(qty_details, list) and len(qty_details) >= 2:
            qualifier = qty_details[0]
            value = qty_details[1]
            unit = qty_details[2] if len(qty_details) > 2 else "KWH"
            
            # Convert value to float if possible
            try:
                numeric_value = float(value) if value else 0.0
            except (ValueError, TypeError):
                logger.warning(f"Could not convert QTY value to float: {value}")
                numeric_value = 0.0
            
            return {
                "type": "quantity",
                "qualifier": qualifier,
                "value": numeric_value,
                "unit": unit,
                "reading_type": self._determine_reading_type(qualifier),
                "timestamp": datetime.utcnow().isoformat()
            }
        
        logger.warning(f"Invalid QTY segment format: {elements}")
        return {}
    
    def handle(self, segment: Dict[str, Any]) -> Dict[str, Any]:
        """Handle QTY segment"""
        if not self.validate_segment(segment):
            return {}
        return self.handle_QTY(segment)
    
    def _determine_reading_type(self, qualifier: str) -> str:
        """Determine reading type based on QTY qualifier"""
        consumption_qualifiers = ["220", "221", "47", "131"]  # Common consumption qualifiers
        generation_qualifiers = ["222", "223", "48", "132"]   # Common generation qualifiers
        
        if qualifier in consumption_qualifiers:
            return "consumption"
        elif qualifier in generation_qualifiers:
            return "generation"
        else:
            return "consumption"  # Default


class LOCHandler(SegmentHandler):
    """Handler for LOC (Place/Location Identification) segments"""
    
    def __init__(self):
        super().__init__()
        self.segment_type = "LOC"
    
    def handle_LOC(self, segment: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract location data from LOC segment
        
        LOC segment format: LOC+qualifier+location_id:location_name
        """
        elements = segment.get("elements", [])
        
        if len(elements) < 2:
            logger.warning("LOC segment has insufficient elements")
            return {}
        
        qualifier = elements[0]
        location_info = elements[1]
        
        # Parse location information
        if isinstance(location_info, list):
            location_id = location_info[0] if len(location_info) > 0 else None
            location_name = location_info[1] if len(location_info) > 1 else None
        else:
            location_id = location_info
            location_name = None
        
        return {
            "type": "location",
            "qualifier": qualifier,
            "location_id": location_id,
            "location_name": location_name,
            "location_type": self._determine_location_type(qualifier)
        }
    
    def handle(self, segment: Dict[str, Any]) -> Dict[str, Any]:
        """Handle LOC segment"""
        if not self.validate_segment(segment):
            return {}
        return self.handle_LOC(segment)
    
    def _determine_location_type(self, qualifier: str) -> str:
        """Determine location type based on qualifier"""
        location_types = {
            "172": "metering_point",
            "92": "delivery_point",
            "91": "consumption_point",
            "7": "place_of_delivery",
            "8": "place_of_departure"
        }
        return location_types.get(qualifier, "unknown")


class DTMHandler(SegmentHandler):
    """Handler for DTM (Date/Time/Period) segments"""
    
    def __init__(self):
        super().__init__()
        self.segment_type = "DTM"
    
    def handle_DTM(self, segment: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract date/time data from DTM segment
        
        DTM segment format: DTM+qualifier:date:format
        """
        elements = segment.get("elements", [])
        
        if not elements:
            logger.warning("DTM segment has no elements")
            return {}
        
        # First element should contain date/time details
        dtm_details = elements[0] if elements else []
        
        if isinstance(dtm_details, list) and len(dtm_details) >= 2:
            qualifier = dtm_details[0]
            date_value = dtm_details[1]
            format_code = dtm_details[2] if len(dtm_details) > 2 else "102"  # Default CCYYMMDD
            
            # Parse date based on format
            parsed_date = self._parse_date(date_value, format_code)
            
            return {
                "type": "datetime",
                "qualifier": qualifier,
                "raw_value": date_value,
                "format_code": format_code,
                "parsed_date": parsed_date,
                "date_type": self._determine_date_type(qualifier)
            }
        
        logger.warning(f"Invalid DTM segment format: {elements}")
        return {}
    
    def handle(self, segment: Dict[str, Any]) -> Dict[str, Any]:
        """Handle DTM segment"""
        if not self.validate_segment(segment):
            return {}
        return self.handle_DTM(segment)
    
    def _parse_date(self, date_value: str, format_code: str) -> Optional[str]:
        """Parse date value based on format code"""
        if not date_value:
            return None
        
        try:
            if format_code == "102":  # CCYYMMDD
                if len(date_value) == 8:
                    year = int(date_value[:4])
                    month = int(date_value[4:6])
                    day = int(date_value[6:8])
                    return datetime(year, month, day).isoformat()
            elif format_code == "203":  # CCYYMMDDHHMM
                if len(date_value) == 12:
                    year = int(date_value[:4])
                    month = int(date_value[4:6])
                    day = int(date_value[6:8])
                    hour = int(date_value[8:10])
                    minute = int(date_value[10:12])
                    return datetime(year, month, day, hour, minute).isoformat()
            elif format_code == "204":  # CCYYMMDDHHMMSS
                if len(date_value) == 14:
                    year = int(date_value[:4])
                    month = int(date_value[4:6])
                    day = int(date_value[6:8])
                    hour = int(date_value[8:10])
                    minute = int(date_value[10:12])
                    second = int(date_value[12:14])
                    return datetime(year, month, day, hour, minute, second).isoformat()
        except (ValueError, IndexError) as e:
            logger.warning(f"Could not parse date {date_value} with format {format_code}: {e}")
        
        return None
    
    def _determine_date_type(self, qualifier: str) -> str:
        """Determine date type based on qualifier"""
        date_types = {
            "137": "document_date",
            "163": "processing_date",
            "194": "start_date",
            "206": "end_date",
            "273": "validity_date"
        }
        return date_types.get(qualifier, "unknown")


class MEAHandler(SegmentHandler):
    """Handler for MEA (Measurements) segments"""
    
    def __init__(self):
        super().__init__()
        self.segment_type = "MEA"
    
    def handle_MEA(self, segment: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract measurement data from MEA segment
        
        MEA segment format: MEA+qualifier+measurement_details+value:unit
        """
        elements = segment.get("elements", [])
        
        if len(elements) < 2:
            logger.warning("MEA segment has insufficient elements")
            return {}
        
        qualifier = elements[0]
        measurement_details = elements[1] if len(elements) > 1 else None
        value_info = elements[2] if len(elements) > 2 else None
        
        # Parse measurement value
        measurement_value = None
        measurement_unit = None
        
        if isinstance(value_info, list) and len(value_info) >= 1:
            try:
                measurement_value = float(value_info[0])
                measurement_unit = value_info[1] if len(value_info) > 1 else "KWH"
            except (ValueError, TypeError):
                logger.warning(f"Could not parse MEA value: {value_info}")
        
        return {
            "type": "measurement",
            "qualifier": qualifier,
            "measurement_details": measurement_details,
            "value": measurement_value,
            "unit": measurement_unit,
            "measurement_type": self._determine_measurement_type(qualifier)
        }
    
    def handle(self, segment: Dict[str, Any]) -> Dict[str, Any]:
        """Handle MEA segment"""
        if not self.validate_segment(segment):
            return {}
        return self.handle_MEA(segment)
    
    def _determine_measurement_type(self, qualifier: str) -> str:
        """Determine measurement type based on qualifier"""
        measurement_types = {
            "AAE": "energy_consumption",
            "AAF": "energy_generation",
            "AAG": "power_consumption",
            "AAH": "power_generation"
        }
        return measurement_types.get(qualifier, "unknown")


class NADHandler(SegmentHandler):
    """Handler for NAD (Name and Address) segments"""
    
    def __init__(self):
        super().__init__()
        self.segment_type = "NAD"
    
    def handle_NAD(self, segment: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract name and address data from NAD segment
        
        NAD segment format: NAD+qualifier+party_id+name+address
        """
        elements = segment.get("elements", [])
        
        if not elements:
            logger.warning("NAD segment has no elements")
            return {}
        
        qualifier = elements[0]
        party_id = elements[1] if len(elements) > 1 else None
        name_info = elements[2] if len(elements) > 2 else None
        address_info = elements[3] if len(elements) > 3 else None
        
        return {
            "type": "party",
            "qualifier": qualifier,
            "party_id": party_id,
            "name": name_info,
            "address": address_info,
            "party_role": self._determine_party_role(qualifier)
        }
    
    def handle(self, segment: Dict[str, Any]) -> Dict[str, Any]:
        """Handle NAD segment"""
        if not self.validate_segment(segment):
            return {}
        return self.handle_NAD(segment)
    
    def _determine_party_role(self, qualifier: str) -> str:
        """Determine party role based on qualifier"""
        party_roles = {
            "MS": "message_sender",
            "MR": "message_recipient",
            "SU": "supplier",
            "DP": "distribution_partner",
            "UD": "utility_distributor",
            "ZSO": "system_operator"
        }
        return party_roles.get(qualifier, "unknown")


class SegmentHandlerFactory:
    """Factory for creating segment handlers"""
    
    def __init__(self):
        self.handlers = {
            "QTY": QTYHandler(),
            "LOC": LOCHandler(),
            "DTM": DTMHandler(),
            "MEA": MEAHandler(),
            "NAD": NADHandler()
        }
    
    def get_handler(self, segment_type: str) -> Optional[SegmentHandler]:
        """Get handler for specific segment type"""
        return self.handlers.get(segment_type)
    
    def handle_segment(self, segment: Dict[str, Any]) -> Dict[str, Any]:
        """Handle a segment using appropriate handler"""
        segment_type = segment.get("tag")
        
        if not segment_type:
            logger.warning("Segment has no tag")
            return {}
        
        handler = self.get_handler(segment_type)
        
        if handler:
            return handler.handle(segment)
        else:
            logger.info(f"No handler available for segment type: {segment_type}")
            return {
                "type": "unhandled",
                "segment_type": segment_type,
                "raw_data": segment
            }
    
    def process_segments(self, segments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process multiple segments"""
        processed_segments = []
        
        for segment in segments:
            processed = self.handle_segment(segment)
            if processed:
                processed_segments.append(processed)
        
        return processed_segments


# Convenience functions
def handle_QTY(segment: Dict[str, Any]) -> Dict[str, Any]:
    """Handle QTY segment"""
    handler = QTYHandler()
    return handler.handle(segment)


def handle_LOC(segment: Dict[str, Any]) -> Dict[str, Any]:
    """Handle LOC segment"""
    handler = LOCHandler()
    return handler.handle(segment)


def handle_DTM(segment: Dict[str, Any]) -> Dict[str, Any]:
    """Handle DTM segment"""
    handler = DTMHandler()
    return handler.handle(segment)


def handle_MEA(segment: Dict[str, Any]) -> Dict[str, Any]:
    """Handle MEA segment"""
    handler = MEAHandler()
    return handler.handle(segment)


def handle_NAD(segment: Dict[str, Any]) -> Dict[str, Any]:
    """Handle NAD segment"""
    handler = NADHandler()
    return handler.handle(segment)


# Default factory instance
default_factory = SegmentHandlerFactory()
