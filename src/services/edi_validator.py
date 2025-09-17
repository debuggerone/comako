"""
EDI@Energy Specification Validator

This module provides validation functionality for EDI messages according to
the EDI@Energy specification used in the German energy market.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import re

logger = logging.getLogger(__name__)


class EDIValidationError(Exception):
    """Custom exception for EDI validation errors."""
    pass


class EDIValidationRule:
    """
    Represents a single EDI validation rule.
    
    Contains the rule definition, validation logic, and error handling.
    """
    
    def __init__(
        self,
        rule_id: str,
        description: str,
        severity: str = "error",
        message_types: Optional[List[str]] = None
    ):
        """
        Initialize validation rule.
        
        Args:
            rule_id: Unique rule identifier
            description: Human-readable rule description
            severity: Rule severity (error, warning, info)
            message_types: List of message types this rule applies to
        """
        self.rule_id = rule_id
        self.description = description
        self.severity = severity
        self.message_types = message_types or []
    
    def applies_to(self, message_type: str) -> bool:
        """Check if rule applies to given message type."""
        return not self.message_types or message_type in self.message_types
    
    def validate(self, message_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Validate message against this rule.
        
        Args:
            message_data: Parsed EDI message data
            
        Returns:
            List of validation issues found
        """
        # Override in subclasses
        return []


class StructuralValidationRule(EDIValidationRule):
    """Validates EDI message structure and required segments."""
    
    def __init__(self):
        super().__init__(
            rule_id="STRUCT_001",
            description="Validate required EDI segments are present",
            severity="error"
        )
    
    def validate(self, message_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Validate message structure."""
        issues = []
        
        # Check for required segments
        required_segments = ["UNB", "UNH", "UNT", "UNZ"]
        
        for segment in required_segments:
            if segment not in message_data:
                issues.append({
                    "rule_id": self.rule_id,
                    "severity": self.severity,
                    "message": f"Missing required segment: {segment}",
                    "segment": segment,
                    "description": self.description
                })
        
        # Check segment order
        if "UNB" in message_data and "UNZ" in message_data:
            # UNB should come before UNZ
            segments = list(message_data.keys())
            unb_index = segments.index("UNB") if "UNB" in segments else -1
            unz_index = segments.index("UNZ") if "UNZ" in segments else -1
            
            if unb_index >= 0 and unz_index >= 0 and unb_index >= unz_index:
                issues.append({
                    "rule_id": self.rule_id,
                    "severity": "warning",
                    "message": "UNB segment should come before UNZ segment",
                    "segment": "UNB/UNZ",
                    "description": "Segment order validation"
                })
        
        return issues


class MessageTypeValidationRule(EDIValidationRule):
    """Validates message type specific requirements."""
    
    def __init__(self):
        super().__init__(
            rule_id="MSGTYPE_001",
            description="Validate message type specific segments",
            severity="error"
        )
    
    def validate(self, message_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Validate message type requirements."""
        issues = []
        
        # Extract message type from UNH segment
        message_type = self._extract_message_type(message_data)
        
        if message_type == "UTILMD":
            issues.extend(self._validate_utilmd(message_data))
        elif message_type == "MSCONS":
            issues.extend(self._validate_mscons(message_data))
        elif message_type == "APERAK":
            issues.extend(self._validate_aperak(message_data))
        
        return issues
    
    def _extract_message_type(self, message_data: Dict[str, Any]) -> str:
        """Extract message type from UNH segment."""
        unh = message_data.get("UNH", [])
        if len(unh) >= 3:
            message_info = unh[2].split(":") if isinstance(unh[2], str) else unh[2]
            if isinstance(message_info, list) and len(message_info) > 0:
                return message_info[0]
        return "UNKNOWN"
    
    def _validate_utilmd(self, message_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Validate UTILMD specific requirements."""
        issues = []
        
        # UTILMD should have BGM, DTM, NAD segments
        required_segments = ["BGM", "DTM", "NAD"]
        
        for segment in required_segments:
            if segment not in message_data:
                issues.append({
                    "rule_id": self.rule_id,
                    "severity": self.severity,
                    "message": f"UTILMD missing required segment: {segment}",
                    "segment": segment,
                    "description": f"UTILMD requires {segment} segment"
                })
        
        return issues
    
    def _validate_mscons(self, message_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Validate MSCONS specific requirements."""
        issues = []
        
        # MSCONS should have BGM, DTM, NAD, LOC segments
        required_segments = ["BGM", "DTM", "NAD", "LOC"]
        
        for segment in required_segments:
            if segment not in message_data:
                issues.append({
                    "rule_id": self.rule_id,
                    "severity": self.severity,
                    "message": f"MSCONS missing required segment: {segment}",
                    "segment": segment,
                    "description": f"MSCONS requires {segment} segment"
                })
        
        return issues
    
    def _validate_aperak(self, message_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Validate APERAK specific requirements."""
        issues = []
        
        # APERAK should have BGM segment
        if "BGM" not in message_data:
            issues.append({
                "rule_id": self.rule_id,
                "severity": self.severity,
                "message": "APERAK missing required BGM segment",
                "segment": "BGM",
                "description": "APERAK requires BGM segment"
            })
        
        return issues


class DataElementValidationRule(EDIValidationRule):
    """Validates data elements within segments."""
    
    def __init__(self):
        super().__init__(
            rule_id="DATA_001",
            description="Validate data element formats and values",
            severity="error"
        )
    
    def validate(self, message_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Validate data elements."""
        issues = []
        
        # Validate UNB segment
        if "UNB" in message_data:
            issues.extend(self._validate_unb_segment(message_data["UNB"]))
        
        # Validate UNH segment
        if "UNH" in message_data:
            issues.extend(self._validate_unh_segment(message_data["UNH"]))
        
        # Validate DTM segment
        if "DTM" in message_data:
            issues.extend(self._validate_dtm_segment(message_data["DTM"]))
        
        return issues
    
    def _validate_unb_segment(self, unb_data: List[str]) -> List[Dict[str, Any]]:
        """Validate UNB segment data elements."""
        issues = []
        
        if len(unb_data) < 5:
            issues.append({
                "rule_id": self.rule_id,
                "severity": self.severity,
                "message": "UNB segment has insufficient data elements",
                "segment": "UNB",
                "description": "UNB requires at least 5 data elements"
            })
            return issues
        
        # Validate syntax identifier
        syntax_id = unb_data[0]
        if not syntax_id.startswith("UNOC"):
            issues.append({
                "rule_id": self.rule_id,
                "severity": "warning",
                "message": f"Unexpected syntax identifier: {syntax_id}",
                "segment": "UNB",
                "description": "UNB syntax identifier validation"
            })
        
        # Validate sender/receiver IDs (should not be empty)
        if not unb_data[1]:
            issues.append({
                "rule_id": self.rule_id,
                "severity": self.severity,
                "message": "UNB sender ID is empty",
                "segment": "UNB",
                "description": "UNB sender ID validation"
            })
        
        if not unb_data[2]:
            issues.append({
                "rule_id": self.rule_id,
                "severity": self.severity,
                "message": "UNB receiver ID is empty",
                "segment": "UNB",
                "description": "UNB receiver ID validation"
            })
        
        return issues
    
    def _validate_unh_segment(self, unh_data: List[str]) -> List[Dict[str, Any]]:
        """Validate UNH segment data elements."""
        issues = []
        
        if len(unh_data) < 3:
            issues.append({
                "rule_id": self.rule_id,
                "severity": self.severity,
                "message": "UNH segment has insufficient data elements",
                "segment": "UNH",
                "description": "UNH requires at least 3 data elements"
            })
            return issues
        
        # Validate message reference number
        if not unh_data[1]:
            issues.append({
                "rule_id": self.rule_id,
                "severity": self.severity,
                "message": "UNH message reference number is empty",
                "segment": "UNH",
                "description": "UNH message reference validation"
            })
        
        return issues
    
    def _validate_dtm_segment(self, dtm_data: List[str]) -> List[Dict[str, Any]]:
        """Validate DTM segment data elements."""
        issues = []
        
        if len(dtm_data) < 3:
            return issues  # DTM is optional, so don't error if missing
        
        # Validate date format
        date_value = dtm_data[2] if len(dtm_data) > 2 else dtm_data[1]
        if date_value and not self._is_valid_date_format(date_value):
            issues.append({
                "rule_id": self.rule_id,
                "severity": "warning",
                "message": f"DTM date format may be invalid: {date_value}",
                "segment": "DTM",
                "description": "DTM date format validation"
            })
        
        return issues
    
    def _is_valid_date_format(self, date_str: str) -> bool:
        """Check if date string matches expected formats."""
        # Common EDI date formats: YYYYMMDD, YYMMDD, YYYYMMDDHHMM
        patterns = [
            r'^\d{8}$',      # YYYYMMDD
            r'^\d{6}$',      # YYMMDD
            r'^\d{12}$',     # YYYYMMDDHHMM
            r'^\d{4}-\d{2}-\d{2}$'  # YYYY-MM-DD
        ]
        
        return any(re.match(pattern, date_str) for pattern in patterns)


class BusinessRuleValidationRule(EDIValidationRule):
    """Validates business logic and constraints."""
    
    def __init__(self):
        super().__init__(
            rule_id="BUSINESS_001",
            description="Validate business rules and constraints",
            severity="warning"
        )
    
    def validate(self, message_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Validate business rules."""
        issues = []
        
        # Validate quantity values
        if "QTY" in message_data:
            issues.extend(self._validate_quantities(message_data["QTY"]))
        
        # Validate measurement values
        if "MEA" in message_data:
            issues.extend(self._validate_measurements(message_data["MEA"]))
        
        # Validate location codes
        if "LOC" in message_data:
            issues.extend(self._validate_locations(message_data["LOC"]))
        
        return issues
    
    def _validate_quantities(self, qty_data: List[str]) -> List[Dict[str, Any]]:
        """Validate quantity values."""
        issues = []
        
        if len(qty_data) >= 3:
            try:
                quantity = float(qty_data[2])
                if quantity < 0:
                    issues.append({
                        "rule_id": self.rule_id,
                        "severity": "warning",
                        "message": f"Negative quantity value: {quantity}",
                        "segment": "QTY",
                        "description": "Quantity should not be negative"
                    })
                elif quantity > 1000000:  # Arbitrary large value check
                    issues.append({
                        "rule_id": self.rule_id,
                        "severity": "warning",
                        "message": f"Unusually large quantity: {quantity}",
                        "segment": "QTY",
                        "description": "Quantity value seems unusually large"
                    })
            except ValueError:
                issues.append({
                    "rule_id": self.rule_id,
                    "severity": self.severity,
                    "message": f"Invalid quantity format: {qty_data[2]}",
                    "segment": "QTY",
                    "description": "Quantity must be numeric"
                })
        
        return issues
    
    def _validate_measurements(self, mea_data: List[str]) -> List[Dict[str, Any]]:
        """Validate measurement values."""
        issues = []
        
        if len(mea_data) >= 4:
            try:
                measurement = float(mea_data[3])
                if measurement < 0:
                    issues.append({
                        "rule_id": self.rule_id,
                        "severity": "warning",
                        "message": f"Negative measurement value: {measurement}",
                        "segment": "MEA",
                        "description": "Measurement should not be negative"
                    })
            except ValueError:
                issues.append({
                    "rule_id": self.rule_id,
                    "severity": "warning",
                    "message": f"Invalid measurement format: {mea_data[3]}",
                    "segment": "MEA",
                    "description": "Measurement must be numeric"
                })
        
        return issues
    
    def _validate_locations(self, loc_data: List[str]) -> List[Dict[str, Any]]:
        """Validate location codes."""
        issues = []
        
        if len(loc_data) >= 3:
            location_id = loc_data[2]
            # Basic validation - location ID should not be empty
            if not location_id:
                issues.append({
                    "rule_id": self.rule_id,
                    "severity": "warning",
                    "message": "Location ID is empty",
                    "segment": "LOC",
                    "description": "Location ID should be specified"
                })
        
        return issues


class EDIEnergyValidator:
    """
    Main EDI@Energy specification validator.
    
    Validates EDI messages according to EDI@Energy standards used in
    the German energy market.
    """
    
    def __init__(self):
        """Initialize validator with standard rules."""
        self.rules = [
            StructuralValidationRule(),
            MessageTypeValidationRule(),
            DataElementValidationRule(),
            BusinessRuleValidationRule()
        ]
        self.validation_stats = {
            "messages_validated": 0,
            "total_issues": 0,
            "errors": 0,
            "warnings": 0,
            "info": 0
        }
    
    def add_rule(self, rule: EDIValidationRule) -> None:
        """Add custom validation rule."""
        self.rules.append(rule)
        logger.info(f"Added validation rule: {rule.rule_id}")
    
    def validate_message(self, message_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate EDI message against all rules.
        
        Args:
            message_data: Parsed EDI message data
            
        Returns:
            Validation result with issues and statistics
        """
        start_time = datetime.now()
        all_issues = []
        
        # Extract message type for rule filtering
        message_type = self._extract_message_type(message_data)
        
        # Run all applicable rules
        for rule in self.rules:
            if rule.applies_to(message_type):
                try:
                    issues = rule.validate(message_data)
                    all_issues.extend(issues)
                except Exception as e:
                    logger.error(f"Error in validation rule {rule.rule_id}: {e}")
                    all_issues.append({
                        "rule_id": rule.rule_id,
                        "severity": "error",
                        "message": f"Validation rule error: {str(e)}",
                        "segment": "SYSTEM",
                        "description": "Internal validation error"
                    })
        
        # Calculate statistics
        error_count = sum(1 for issue in all_issues if issue["severity"] == "error")
        warning_count = sum(1 for issue in all_issues if issue["severity"] == "warning")
        info_count = sum(1 for issue in all_issues if issue["severity"] == "info")
        
        # Update global stats
        self.validation_stats["messages_validated"] += 1
        self.validation_stats["total_issues"] += len(all_issues)
        self.validation_stats["errors"] += error_count
        self.validation_stats["warnings"] += warning_count
        self.validation_stats["info"] += info_count
        
        validation_time = (datetime.now() - start_time).total_seconds()
        
        result = {
            "valid": error_count == 0,
            "message_type": message_type,
            "issues": all_issues,
            "statistics": {
                "total_issues": len(all_issues),
                "errors": error_count,
                "warnings": warning_count,
                "info": info_count,
                "validation_time_seconds": validation_time
            },
            "validated_at": datetime.now().isoformat()
        }
        
        logger.info(f"Validated {message_type} message: {len(all_issues)} issues found")
        return result
    
    def _extract_message_type(self, message_data: Dict[str, Any]) -> str:
        """Extract message type from message data."""
        unh = message_data.get("UNH", [])
        if len(unh) >= 3:
            message_info = unh[2].split(":") if isinstance(unh[2], str) else unh[2]
            if isinstance(message_info, list) and len(message_info) > 0:
                return message_info[0]
        return "UNKNOWN"
    
    def get_validation_statistics(self) -> Dict[str, Any]:
        """Get validation statistics."""
        return self.validation_stats.copy()
    
    def reset_statistics(self) -> None:
        """Reset validation statistics."""
        self.validation_stats = {
            "messages_validated": 0,
            "total_issues": 0,
            "errors": 0,
            "warnings": 0,
            "info": 0
        }


# Convenience functions
def validate_edi_message(message_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate EDI message using default validator.
    
    Args:
        message_data: Parsed EDI message data
        
    Returns:
        Validation result
    """
    validator = EDIEnergyValidator()
    return validator.validate_message(message_data)


def validate_edi_file(file_content: str) -> Dict[str, Any]:
    """
    Parse and validate EDI file content.
    
    Args:
        file_content: Raw EDI file content
        
    Returns:
        Validation result
    """
    from .edi_parser import EDIFACTParser
    
    try:
        parser = EDIFACTParser()
        parsed_data = parser.parse_edi_file(file_content)
        return validate_edi_message(parsed_data)
    except Exception as e:
        return {
            "valid": False,
            "message_type": "UNKNOWN",
            "issues": [{
                "rule_id": "PARSE_ERROR",
                "severity": "error",
                "message": f"Failed to parse EDI file: {str(e)}",
                "segment": "FILE",
                "description": "EDI file parsing error"
            }],
            "statistics": {
                "total_issues": 1,
                "errors": 1,
                "warnings": 0,
                "info": 0,
                "validation_time_seconds": 0
            },
            "validated_at": datetime.now().isoformat()
        }


def create_validation_report(validation_result: Dict[str, Any]) -> str:
    """
    Create human-readable validation report.
    
    Args:
        validation_result: Result from validate_edi_message
        
    Returns:
        Formatted validation report
    """
    report_lines = []
    
    # Header
    report_lines.append("=" * 60)
    report_lines.append("EDI@Energy Validation Report")
    report_lines.append("=" * 60)
    report_lines.append(f"Message Type: {validation_result['message_type']}")
    report_lines.append(f"Validation Status: {'✅ VALID' if validation_result['valid'] else '❌ INVALID'}")
    report_lines.append(f"Validated At: {validation_result['validated_at']}")
    report_lines.append("")
    
    # Statistics
    stats = validation_result['statistics']
    report_lines.append("Statistics:")
    report_lines.append(f"  Total Issues: {stats['total_issues']}")
    report_lines.append(f"  Errors: {stats['errors']}")
    report_lines.append(f"  Warnings: {stats['warnings']}")
    report_lines.append(f"  Info: {stats['info']}")
    report_lines.append(f"  Validation Time: {stats['validation_time_seconds']:.3f}s")
    report_lines.append("")
    
    # Issues
    if validation_result['issues']:
        report_lines.append("Issues Found:")
        report_lines.append("-" * 40)
        
        for i, issue in enumerate(validation_result['issues'], 1):
            severity_icon = {
                "error": "❌",
                "warning": "⚠️",
                "info": "ℹ️"
            }.get(issue['severity'], "❓")
            
            report_lines.append(f"{i}. {severity_icon} {issue['severity'].upper()}")
            report_lines.append(f"   Rule: {issue['rule_id']}")
            report_lines.append(f"   Segment: {issue['segment']}")
            report_lines.append(f"   Message: {issue['message']}")
            report_lines.append(f"   Description: {issue['description']}")
            report_lines.append("")
    else:
        report_lines.append("✅ No issues found - message is valid!")
    
    report_lines.append("=" * 60)
    
    return "\n".join(report_lines)


# Example usage and testing
def demo_edi_validation():
    """Demonstrate EDI validation functionality."""
    
    print("=== EDI@Energy Validation Demo ===")
    
    # Test with valid UTILMD message
    print("\n1. Testing valid UTILMD message...")
    valid_utilmd = {
        'UNB': ['UNOC:3', 'SENDER123', 'COMAKO', '250103:1200', 'REF001'],
        'UNH': ['MSG001', 'UTILMD:D:03B:UN:EEG', '1.1e'],
        'BGM': ['E01', 'DOC123', '9'],
        'DTM': ['137', '20250103', '102'],
        'NAD': ['MS', 'COMPANY123', 'Energy Corp'],
        'LOC': ['172', 'MP001', 'Metering Point 1'],
        'QTY': ['220', '1500.5', 'KWH'],
        'UNT': ['7', 'MSG001'],
        'UNZ': ['1', 'REF001']
    }
    
    result = validate_edi_message(valid_utilmd)
    print(f"   Valid: {result['valid']}")
    print(f"   Issues: {result['statistics']['total_issues']}")
    
    # Test with invalid message
    print("\n2. Testing invalid message...")
    invalid_message = {
        'UNH': ['MSG002', 'UTILMD:D:03B:UN:EEG', '1.1e'],
        'QTY': ['220', 'INVALID_NUMBER', 'KWH'],
        'UNT': ['3', 'MSG002']
    }
    
    result = validate_edi_message(invalid_message)
    print(f"   Valid: {result['valid']}")
    print(f"   Issues: {result['statistics']['total_issues']}")
    print(f"   Errors: {result['statistics']['errors']}")
    print(f"   Warnings: {result['statistics']['warnings']}")
    
    # Show validation report
    print("\n3. Sample validation report...")
    report = create_validation_report(result)
    print(report[:500] + "..." if len(report) > 500 else report)
    
    print("\n=== EDI Validation Demo Complete ===")


if __name__ == "__main__":
    # Run demo
    demo_edi_validation()
