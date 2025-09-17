"""
EDI to JSON Conversion Pipeline

This module provides functionality to convert parsed EDI data structures
into standardized JSON format for internal processing.
"""

import json
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)


class EDIConverter:
    """
    Converts parsed EDI data structures to JSON format.
    
    Handles transformation of EDIFACT segments into structured JSON
    that can be easily processed by downstream systems.
    """
    
    def __init__(self):
        self.segment_mappings = {
            'UNB': self._convert_unb_segment,
            'UNH': self._convert_unh_segment,
            'BGM': self._convert_bgm_segment,
            'DTM': self._convert_dtm_segment,
            'NAD': self._convert_nad_segment,
            'LOC': self._convert_loc_segment,
            'QTY': self._convert_qty_segment,
            'MEA': self._convert_mea_segment,
            'UNT': self._convert_unt_segment,
            'UNZ': self._convert_unz_segment,
        }
    
    def convert_to_json(self, edi_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert parsed EDI data to JSON format.
        
        Args:
            edi_data: Dictionary containing parsed EDI segments
            
        Returns:
            Dictionary with standardized JSON structure
        """
        try:
            json_output = {
                "message_type": self._determine_message_type(edi_data),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "header": {},
                "body": {},
                "segments": [],
                "metadata": {
                    "conversion_version": "1.0",
                    "source_format": "EDIFACT"
                }
            }
            
            # Process each segment
            for segment_name, segment_data in edi_data.items():
                if segment_name in self.segment_mappings:
                    converted_segment = self.segment_mappings[segment_name](segment_data)
                    
                    # Categorize segments
                    if segment_name in ['UNB', 'UNH', 'BGM']:
                        json_output["header"].update(converted_segment)
                    elif segment_name in ['UNT', 'UNZ']:
                        json_output["metadata"].update(converted_segment)
                    else:
                        json_output["body"].update(converted_segment)
                    
                    # Keep detailed segment info
                    json_output["segments"].append({
                        "segment_type": segment_name,
                        "data": converted_segment
                    })
                else:
                    logger.warning(f"Unknown segment type: {segment_name}")
                    json_output["segments"].append({
                        "segment_type": segment_name,
                        "data": segment_data,
                        "status": "unmapped"
                    })
            
            return json_output
            
        except Exception as e:
            logger.error(f"Error converting EDI to JSON: {e}")
            raise ValueError(f"EDI conversion failed: {e}")
    
    def _determine_message_type(self, edi_data: Dict[str, Any]) -> str:
        """Determine the EDI message type from the data."""
        if 'UNH' in edi_data:
            unh_data = edi_data['UNH']
            if isinstance(unh_data, dict) and 'message_type' in unh_data:
                return unh_data['message_type']
            elif isinstance(unh_data, list) and len(unh_data) > 1:
                return unh_data[1]
        
        # Default fallback
        return "UNKNOWN"
    
    def _convert_unb_segment(self, segment_data: Any) -> Dict[str, Any]:
        """Convert UNB (Interchange Header) segment."""
        if isinstance(segment_data, list) and len(segment_data) >= 4:
            return {
                "interchange_header": {
                    "syntax_identifier": segment_data[0] if len(segment_data) > 0 else None,
                    "sender": segment_data[1] if len(segment_data) > 1 else None,
                    "recipient": segment_data[2] if len(segment_data) > 2 else None,
                    "date_time": segment_data[3] if len(segment_data) > 3 else None,
                    "control_reference": segment_data[4] if len(segment_data) > 4 else None
                }
            }
        return {"interchange_header": segment_data}
    
    def _convert_unh_segment(self, segment_data: Any) -> Dict[str, Any]:
        """Convert UNH (Message Header) segment."""
        if isinstance(segment_data, list) and len(segment_data) >= 2:
            return {
                "message_header": {
                    "reference_number": segment_data[0] if len(segment_data) > 0 else None,
                    "message_type": segment_data[1] if len(segment_data) > 1 else None,
                    "version": segment_data[2] if len(segment_data) > 2 else None,
                    "release": segment_data[3] if len(segment_data) > 3 else None
                }
            }
        return {"message_header": segment_data}
    
    def _convert_bgm_segment(self, segment_data: Any) -> Dict[str, Any]:
        """Convert BGM (Beginning of Message) segment."""
        if isinstance(segment_data, list) and len(segment_data) >= 2:
            return {
                "document_info": {
                    "document_name": segment_data[0] if len(segment_data) > 0 else None,
                    "document_number": segment_data[1] if len(segment_data) > 1 else None,
                    "message_function": segment_data[2] if len(segment_data) > 2 else None
                }
            }
        return {"document_info": segment_data}
    
    def _convert_dtm_segment(self, segment_data: Any) -> Dict[str, Any]:
        """Convert DTM (Date/Time) segment."""
        if isinstance(segment_data, list) and len(segment_data) >= 2:
            return {
                "date_time": {
                    "qualifier": segment_data[0] if len(segment_data) > 0 else None,
                    "date": segment_data[1] if len(segment_data) > 1 else None,
                    "format": segment_data[2] if len(segment_data) > 2 else None
                }
            }
        return {"date_time": segment_data}
    
    def _convert_nad_segment(self, segment_data: Any) -> Dict[str, Any]:
        """Convert NAD (Name and Address) segment."""
        if isinstance(segment_data, list) and len(segment_data) >= 2:
            return {
                "party_info": {
                    "qualifier": segment_data[0] if len(segment_data) > 0 else None,
                    "identification": segment_data[1] if len(segment_data) > 1 else None,
                    "name": segment_data[2] if len(segment_data) > 2 else None,
                    "address": segment_data[3] if len(segment_data) > 3 else None
                }
            }
        return {"party_info": segment_data}
    
    def _convert_loc_segment(self, segment_data: Any) -> Dict[str, Any]:
        """Convert LOC (Location) segment."""
        if isinstance(segment_data, list) and len(segment_data) >= 2:
            return {
                "location": {
                    "qualifier": segment_data[0] if len(segment_data) > 0 else None,
                    "identification": segment_data[1] if len(segment_data) > 1 else None,
                    "description": segment_data[2] if len(segment_data) > 2 else None
                }
            }
        return {"location": segment_data}
    
    def _convert_qty_segment(self, segment_data: Any) -> Dict[str, Any]:
        """Convert QTY (Quantity) segment."""
        if isinstance(segment_data, list) and len(segment_data) >= 2:
            return {
                "quantity": {
                    "qualifier": segment_data[0] if len(segment_data) > 0 else None,
                    "value": float(segment_data[1]) if len(segment_data) > 1 and segment_data[1] else None,
                    "unit": segment_data[2] if len(segment_data) > 2 else None
                }
            }
        return {"quantity": segment_data}
    
    def _convert_mea_segment(self, segment_data: Any) -> Dict[str, Any]:
        """Convert MEA (Measurement) segment."""
        if isinstance(segment_data, list) and len(segment_data) >= 3:
            return {
                "measurement": {
                    "qualifier": segment_data[0] if len(segment_data) > 0 else None,
                    "dimension": segment_data[1] if len(segment_data) > 1 else None,
                    "value": float(segment_data[2]) if len(segment_data) > 2 and segment_data[2] else None,
                    "unit": segment_data[3] if len(segment_data) > 3 else None
                }
            }
        return {"measurement": segment_data}
    
    def _convert_unt_segment(self, segment_data: Any) -> Dict[str, Any]:
        """Convert UNT (Message Trailer) segment."""
        if isinstance(segment_data, list) and len(segment_data) >= 2:
            return {
                "message_trailer": {
                    "segment_count": int(segment_data[0]) if len(segment_data) > 0 and segment_data[0] else None,
                    "reference_number": segment_data[1] if len(segment_data) > 1 else None
                }
            }
        return {"message_trailer": segment_data}
    
    def _convert_unz_segment(self, segment_data: Any) -> Dict[str, Any]:
        """Convert UNZ (Interchange Trailer) segment."""
        if isinstance(segment_data, list) and len(segment_data) >= 2:
            return {
                "interchange_trailer": {
                    "group_count": int(segment_data[0]) if len(segment_data) > 0 and segment_data[0] else None,
                    "control_reference": segment_data[1] if len(segment_data) > 1 else None
                }
            }
        return {"interchange_trailer": segment_data}


def convert_edi_to_json(edi_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convenience function to convert EDI data to JSON.
    
    Args:
        edi_data: Parsed EDI data structure
        
    Returns:
        JSON representation of the EDI data
    """
    converter = EDIConverter()
    return converter.convert_to_json(edi_data)


def convert_utilmd_to_json(edi_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert UTILMD (Utilities Master Data) message to JSON.
    
    Args:
        edi_data: Parsed UTILMD EDI data
        
    Returns:
        JSON structure optimized for utilities data processing
    """
    converter = EDIConverter()
    json_data = converter.convert_to_json(edi_data)
    
    # Add UTILMD-specific processing
    if json_data.get("message_type") == "UTILMD":
        json_data["utilities_data"] = {
            "metering_points": [],
            "consumption_data": [],
            "meter_readings": []
        }
        
        # Extract utilities-specific information from segments
        for segment in json_data.get("segments", []):
            segment_type = segment.get("segment_type")
            segment_data = segment.get("data", {})
            
            if segment_type == "LOC" and "location" in segment_data:
                json_data["utilities_data"]["metering_points"].append(segment_data["location"])
            elif segment_type == "QTY" and "quantity" in segment_data:
                json_data["utilities_data"]["consumption_data"].append(segment_data["quantity"])
            elif segment_type == "MEA" and "measurement" in segment_data:
                json_data["utilities_data"]["meter_readings"].append(segment_data["measurement"])
    
    return json_data


def convert_mscons_to_json(edi_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert MSCONS (Metered Services Consumption Report) message to JSON.
    
    Args:
        edi_data: Parsed MSCONS EDI data
        
    Returns:
        JSON structure optimized for consumption reporting
    """
    converter = EDIConverter()
    json_data = converter.convert_to_json(edi_data)
    
    # Add MSCONS-specific processing
    if json_data.get("message_type") == "MSCONS":
        json_data["consumption_report"] = {
            "reporting_period": {},
            "meter_readings": [],
            "consumption_totals": []
        }
        
        # Extract consumption-specific information
        for segment in json_data.get("segments", []):
            segment_type = segment.get("segment_type")
            segment_data = segment.get("data", {})
            
            if segment_type == "DTM" and "date_time" in segment_data:
                json_data["consumption_report"]["reporting_period"] = segment_data["date_time"]
            elif segment_type == "QTY" and "quantity" in segment_data:
                json_data["consumption_report"]["consumption_totals"].append(segment_data["quantity"])
            elif segment_type == "MEA" and "measurement" in segment_data:
                json_data["consumption_report"]["meter_readings"].append(segment_data["measurement"])
    
    return json_data


class JSONValidator:
    """
    Validates converted JSON data against expected schemas.
    """
    
    @staticmethod
    def validate_basic_structure(json_data: Dict[str, Any]) -> bool:
        """
        Validate that JSON has required basic structure.
        
        Args:
            json_data: Converted JSON data
            
        Returns:
            True if structure is valid, False otherwise
        """
        required_fields = ["message_type", "timestamp", "header", "body", "segments", "metadata"]
        return all(field in json_data for field in required_fields)
    
    @staticmethod
    def validate_utilmd_structure(json_data: Dict[str, Any]) -> bool:
        """
        Validate UTILMD-specific JSON structure.
        
        Args:
            json_data: Converted UTILMD JSON data
            
        Returns:
            True if UTILMD structure is valid, False otherwise
        """
        if not JSONValidator.validate_basic_structure(json_data):
            return False
        
        if json_data.get("message_type") != "UTILMD":
            return False
        
        utilities_data = json_data.get("utilities_data", {})
        required_utilities_fields = ["metering_points", "consumption_data", "meter_readings"]
        return all(field in utilities_data for field in required_utilities_fields)
    
    @staticmethod
    def validate_mscons_structure(json_data: Dict[str, Any]) -> bool:
        """
        Validate MSCONS-specific JSON structure.
        
        Args:
            json_data: Converted MSCONS JSON data
            
        Returns:
            True if MSCONS structure is valid, False otherwise
        """
        if not JSONValidator.validate_basic_structure(json_data):
            return False
        
        if json_data.get("message_type") != "MSCONS":
            return False
        
        consumption_report = json_data.get("consumption_report", {})
        required_report_fields = ["reporting_period", "meter_readings", "consumption_totals"]
        return all(field in consumption_report for field in required_report_fields)


def pretty_print_json(json_data: Dict[str, Any], indent: int = 2) -> str:
    """
    Pretty print JSON data for debugging and logging.
    
    Args:
        json_data: JSON data to format
        indent: Number of spaces for indentation
        
    Returns:
        Formatted JSON string
    """
    return json.dumps(json_data, indent=indent, ensure_ascii=False, default=str)
