import re
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from enum import Enum


logger = logging.getLogger(__name__)


class EDIMessageType(str, Enum):
    """Enumeration of supported EDI message types"""
    UTILMD = "UTILMD"  # Utilities master data message
    MSCONS = "MSCONS"  # Metered services consumption report message
    APERAK = "APERAK"  # Application error and acknowledgment message


class EDISegmentType(str, Enum):
    """Enumeration of EDI segment types"""
    UNB = "UNB"  # Interchange header
    UNH = "UNH"  # Message header
    BGM = "BGM"  # Beginning of message
    DTM = "DTM"  # Date/time/period
    NAD = "NAD"  # Name and address
    LOC = "LOC"  # Place/location identification
    MEA = "MEA"  # Measurements
    QTY = "QTY"  # Quantity
    CCI = "CCI"  # Characteristic/class id
    CAV = "CAV"  # Characteristic value
    UNT = "UNT"  # Message trailer
    UNZ = "UNZ"  # Interchange trailer


class EDIParseError(Exception):
    """Custom exception for EDI parsing errors"""
    pass


class EDIFACTParser:
    """Parser for EDIFACT messages used in energy data exchange"""
    
    def __init__(self):
        self.segment_separator = "'"
        self.element_separator = "+"
        self.component_separator = ":"
        self.escape_character = "?"
        
    def parse_edi_file(self, edi_content: str) -> Dict[str, Any]:
        """
        Parse an EDI file and extract structured data
        
        Args:
            edi_content: Raw EDI file content as string
            
        Returns:
            Dictionary containing parsed EDI data
            
        Raises:
            EDIParseError: If parsing fails
        """
        try:
            # Clean and normalize the content
            normalized_content = self._normalize_content(edi_content)
            
            # Split into segments
            segments = self._split_segments(normalized_content)
            
            # Parse segments
            parsed_segments = []
            for segment in segments:
                if segment.strip():
                    parsed_segment = self._parse_segment(segment)
                    parsed_segments.append(parsed_segment)
            
            # Extract message structure
            message_data = self._extract_message_structure(parsed_segments)
            
            # Validate message structure
            self._validate_message_structure(message_data)
            
            logger.info(f"Successfully parsed EDI message with {len(parsed_segments)} segments")
            return message_data
            
        except Exception as e:
            logger.error(f"Failed to parse EDI file: {e}")
            raise EDIParseError(f"EDI parsing failed: {str(e)}")
    
    def _normalize_content(self, content: str) -> str:
        """Normalize EDI content by removing unnecessary whitespace and line breaks"""
        # Remove line breaks and extra spaces, but preserve segment structure
        normalized = re.sub(r'\r\n|\r|\n', '', content)
        normalized = re.sub(r'\s+', ' ', normalized)
        return normalized.strip()
    
    def _split_segments(self, content: str) -> List[str]:
        """Split EDI content into individual segments"""
        # Split by segment separator, but handle escaped separators
        segments = []
        current_segment = ""
        i = 0
        
        while i < len(content):
            char = content[i]
            
            if char == self.escape_character and i + 1 < len(content):
                # Escaped character, add both escape and next character
                current_segment += char + content[i + 1]
                i += 2
            elif char == self.segment_separator:
                # End of segment
                if current_segment.strip():
                    segments.append(current_segment.strip())
                current_segment = ""
                i += 1
            else:
                current_segment += char
                i += 1
        
        # Add final segment if exists
        if current_segment.strip():
            segments.append(current_segment.strip())
        
        return segments
    
    def _parse_segment(self, segment: str) -> Dict[str, Any]:
        """Parse a single EDI segment into structured data"""
        if not segment:
            raise EDIParseError("Empty segment")
        
        # Split segment into elements
        elements = segment.split(self.element_separator)
        
        if not elements:
            raise EDIParseError(f"Invalid segment format: {segment}")
        
        segment_tag = elements[0]
        segment_elements = elements[1:] if len(elements) > 1 else []
        
        # Parse each element into components
        parsed_elements = []
        for element in segment_elements:
            if self.component_separator in element:
                components = element.split(self.component_separator)
                parsed_elements.append(components)
            else:
                parsed_elements.append(element)
        
        return {
            "tag": segment_tag,
            "elements": parsed_elements,
            "raw": segment
        }
    
    def _extract_message_structure(self, segments: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Extract high-level message structure from parsed segments"""
        message_data = {
            "interchange_header": None,
            "message_header": None,
            "message_type": None,
            "segments": segments,
            "readings": [],
            "metadata": {}
        }
        
        for segment in segments:
            tag = segment["tag"]
            elements = segment["elements"]
            
            if tag == "UNB":
                message_data["interchange_header"] = self._parse_unb_segment(segment)
            elif tag == "UNH":
                message_data["message_header"] = self._parse_unh_segment(segment)
            elif tag == "BGM":
                message_data["message_type"] = self._parse_bgm_segment(segment)
            elif tag == "DTM":
                self._parse_dtm_segment(segment, message_data)
            elif tag == "NAD":
                self._parse_nad_segment(segment, message_data)
            elif tag == "LOC":
                self._parse_loc_segment(segment, message_data)
            elif tag == "MEA":
                self._parse_mea_segment(segment, message_data)
            elif tag == "QTY":
                self._parse_qty_segment(segment, message_data)
        
        return message_data
    
    def _parse_unb_segment(self, segment: Dict[str, Any]) -> Dict[str, Any]:
        """Parse UNB (Interchange Header) segment"""
        elements = segment["elements"]
        
        return {
            "syntax_identifier": elements[0] if len(elements) > 0 else None,
            "sender": elements[1] if len(elements) > 1 else None,
            "recipient": elements[2] if len(elements) > 2 else None,
            "date_time": elements[3] if len(elements) > 3 else None,
            "interchange_control_reference": elements[4] if len(elements) > 4 else None
        }
    
    def _parse_unh_segment(self, segment: Dict[str, Any]) -> Dict[str, Any]:
        """Parse UNH (Message Header) segment"""
        elements = segment["elements"]
        
        return {
            "message_reference_number": elements[0] if len(elements) > 0 else None,
            "message_identifier": elements[1] if len(elements) > 1 else None
        }
    
    def _parse_bgm_segment(self, segment: Dict[str, Any]) -> Dict[str, Any]:
        """Parse BGM (Beginning of Message) segment"""
        elements = segment["elements"]
        
        return {
            "document_name_code": elements[0] if len(elements) > 0 else None,
            "document_number": elements[1] if len(elements) > 1 else None,
            "message_function_code": elements[2] if len(elements) > 2 else None
        }
    
    def _parse_dtm_segment(self, segment: Dict[str, Any], message_data: Dict[str, Any]):
        """Parse DTM (Date/Time/Period) segment"""
        elements = segment["elements"]
        
        if len(elements) > 0 and isinstance(elements[0], list):
            date_info = elements[0]
            qualifier = date_info[0] if len(date_info) > 0 else None
            date_value = date_info[1] if len(date_info) > 1 else None
            format_code = date_info[2] if len(date_info) > 2 else None
            
            if "dates" not in message_data["metadata"]:
                message_data["metadata"]["dates"] = []
            
            message_data["metadata"]["dates"].append({
                "qualifier": qualifier,
                "value": date_value,
                "format": format_code
            })
    
    def _parse_nad_segment(self, segment: Dict[str, Any], message_data: Dict[str, Any]):
        """Parse NAD (Name and Address) segment"""
        elements = segment["elements"]
        
        party_qualifier = elements[0] if len(elements) > 0 else None
        party_id = elements[1] if len(elements) > 1 else None
        
        if "parties" not in message_data["metadata"]:
            message_data["metadata"]["parties"] = []
        
        message_data["metadata"]["parties"].append({
            "qualifier": party_qualifier,
            "identification": party_id
        })
    
    def _parse_loc_segment(self, segment: Dict[str, Any], message_data: Dict[str, Any]):
        """Parse LOC (Place/Location Identification) segment"""
        elements = segment["elements"]
        
        location_qualifier = elements[0] if len(elements) > 0 else None
        location_id = elements[1] if len(elements) > 1 else None
        
        if "locations" not in message_data["metadata"]:
            message_data["metadata"]["locations"] = []
        
        message_data["metadata"]["locations"].append({
            "qualifier": location_qualifier,
            "identification": location_id
        })
    
    def _parse_mea_segment(self, segment: Dict[str, Any], message_data: Dict[str, Any]):
        """Parse MEA (Measurements) segment"""
        elements = segment["elements"]
        
        measurement_qualifier = elements[0] if len(elements) > 0 else None
        measurement_details = elements[1] if len(elements) > 1 else None
        
        measurement_data = {
            "qualifier": measurement_qualifier,
            "details": measurement_details
        }
        
        message_data["readings"].append(measurement_data)
    
    def _parse_qty_segment(self, segment: Dict[str, Any], message_data: Dict[str, Any]):
        """Parse QTY (Quantity) segment"""
        elements = segment["elements"]
        
        if len(elements) > 0 and isinstance(elements[0], list):
            qty_details = elements[0]
            qty_qualifier = qty_details[0] if len(qty_details) > 0 else None
            qty_value = qty_details[1] if len(qty_details) > 1 else None
            qty_unit = qty_details[2] if len(qty_details) > 2 else None
            
            quantity_data = {
                "qualifier": qty_qualifier,
                "value": float(qty_value) if qty_value and qty_value.replace('.', '').replace('-', '').isdigit() else qty_value,
                "unit": qty_unit,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            message_data["readings"].append(quantity_data)
    
    def _validate_message_structure(self, message_data: Dict[str, Any]):
        """Validate the parsed message structure"""
        required_segments = ["UNB", "UNH", "BGM"]
        
        segment_tags = [seg["tag"] for seg in message_data["segments"]]
        
        for required_tag in required_segments:
            if required_tag not in segment_tags:
                raise EDIParseError(f"Missing required segment: {required_tag}")
        
        # Validate message type
        if not message_data.get("message_type"):
            raise EDIParseError("Could not determine message type")
        
        logger.info("EDI message structure validation passed")
    
    def extract_meter_readings(self, parsed_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract meter readings from parsed EDI data
        
        Args:
            parsed_data: Parsed EDI message data
            
        Returns:
            List of meter reading dictionaries
        """
        readings = []
        
        for reading_data in parsed_data.get("readings", []):
            if "value" in reading_data and reading_data["value"] is not None:
                # Extract metering point from locations or parties
                metering_point = self._extract_metering_point(parsed_data)
                
                reading = {
                    "metering_point": metering_point,
                    "timestamp": reading_data.get("timestamp", datetime.utcnow().isoformat()),
                    "value_kwh": reading_data["value"],
                    "reading_type": self._determine_reading_type(reading_data),
                    "source": "EDI",
                    "qualifier": reading_data.get("qualifier"),
                    "unit": reading_data.get("unit", "KWH")
                }
                
                readings.append(reading)
        
        return readings
    
    def _extract_metering_point(self, parsed_data: Dict[str, Any]) -> str:
        """Extract metering point identifier from parsed data"""
        # Try to find metering point in locations
        locations = parsed_data.get("metadata", {}).get("locations", [])
        for location in locations:
            if location.get("qualifier") == "172":  # Metering point location
                return str(location.get("identification", "UNKNOWN"))
        
        # Fallback to first party identification
        parties = parsed_data.get("metadata", {}).get("parties", [])
        if parties:
            return str(parties[0].get("identification", "UNKNOWN"))
        
        return "UNKNOWN"
    
    def _determine_reading_type(self, reading_data: Dict[str, Any]) -> str:
        """Determine reading type based on qualifier"""
        qualifier = reading_data.get("qualifier", "")
        
        if qualifier in ["220", "221"]:  # Consumption qualifiers
            return "consumption"
        elif qualifier in ["222", "223"]:  # Generation qualifiers
            return "generation"
        else:
            return "consumption"  # Default to consumption


# Convenience functions
def parse_edi_file(edi_content: str) -> Dict[str, Any]:
    """Parse an EDI file using the default parser"""
    parser = EDIFACTParser()
    return parser.parse_edi_file(edi_content)


def extract_readings_from_edi(edi_content: str) -> List[Dict[str, Any]]:
    """Extract meter readings from EDI content"""
    parser = EDIFACTParser()
    parsed_data = parser.parse_edi_file(edi_content)
    return parser.extract_meter_readings(parsed_data)
